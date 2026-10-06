-- First short-step preparation owner. It pins complete immutable approved
-- revisions selected by ID; no Candidate/Timesheet history is rebuilt.
-- This does not yet reserve money or make a run reviewable.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_claim_prepare_job_v1(
  p_job_id uuid,p_lease_seconds integer default 120
) returns table(lease_nonce uuid,owner_epoch bigint)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_job private.bpay_next_job%rowtype;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_run_id uuid;
  v_module_epoch bigint;
  v_channel text;
  v_umbrella_id uuid;
  v_umbrella_vat_chargeable boolean;
  v_owner_epoch bigint;
  v_nonce uuid;
begin
  if p_job_id is null or p_lease_seconds not between 1 and 120 then
    raise exception using errcode='22023',
      message='BPAY_NEXT_PREPARE_LEASE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select j.candidate_id,rc.run_id into strict v_job.candidate_id,v_run_id
    from private.bpay_next_job j
    join private.bpay_next_run_command rc on rc.command_id=j.command_id
    where j.id=p_job_id and j.job_kind='PREPARE';
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for share;
  insert into private.bpay_next_worker_control(candidate_id)
    values(v_job.candidate_id) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.module_epoch<>v_module_epoch
     or v_job.status not in ('READY','LEASED')
     or (v_job.status='LEASED'
       and v_job.lease_until_utc>pg_catalog.clock_timestamp()) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_PREPARE_JOB_NOT_CLAIMABLE';
  end if;
  if exists(select 1 from private.bpay_next_job earlier
      where earlier.candidate_id=v_job.candidate_id
        and earlier.command_sequence<v_job.command_sequence
        and earlier.status<>'DONE') then
    raise exception using errcode='55000',
      message='BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING';
  end if;
  select * into v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if not found then
    select upper(c.pay_method),c.umbrella_id,u.vat_chargeable
      into strict v_channel,v_umbrella_id,v_umbrella_vat_chargeable
      from public.candidates c
      left join public.umbrellas u on u.id=c.umbrella_id
      where c.id=v_job.candidate_id for share of c;
    if v_channel not in ('PAYE','UMBRELLA') then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_PAY_CHANNEL_UNSUPPORTED';
    end if;
    insert into private.bpay_next_run_worker
      (run_id,candidate_id,target_pay_channel,target_umbrella_id,
       target_umbrella_vat_chargeable,status)
      values(v_run_id,v_job.candidate_id,v_channel,
        case when v_channel='UMBRELLA' then v_umbrella_id else null end,
        case when v_channel='UMBRELLA' then v_umbrella_vat_chargeable
          else null end,'PREPARING')
      returning * into v_worker;
  elsif v_worker.status<>'PREPARING' then
    raise exception using errcode='55000',
      message='BPAY_NEXT_PREPARE_WORKER_STATE_CHANGED';
  end if;
  update private.bpay_next_worker_control
    set active_owner_epoch=active_owner_epoch+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_job.candidate_id
    returning active_owner_epoch into v_owner_epoch;
  v_nonce:=pg_catalog.gen_random_uuid();
  update private.bpay_next_job
    set status='LEASED',owner_epoch=v_owner_epoch,lease_nonce=v_nonce,
        lease_until_utc=pg_catalog.clock_timestamp()
          +pg_catalog.make_interval(secs=>p_lease_seconds),
        attempt_count=attempt_count+1
    where id=p_job_id;
  lease_nonce:=v_nonce;owner_epoch:=v_owner_epoch;
  return next;
end
$function$;

create or replace function private.bpay_next_pin_selected_work_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_cursor bigint
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare
  v_job private.bpay_next_job%rowtype;
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker_id uuid;
  v_worker private.bpay_next_run_worker%rowtype;
  v_selection private.bpay_next_run_selection%rowtype;
  v_choice private.bpay_next_work_choice%rowtype;
  v_work private.bpay_next_work%rowtype;
  v_revision private.bpay_next_work_revision%rowtype;
  v_current_cursor bigint;
  v_module_epoch bigint;
  v_query_admitted boolean:=false;
  v_source_classified boolean;
  v_source_origin record;
  v_query_gate jsonb;
  v_query_scope jsonb;
  v_query_disposition text;
  v_query_digest bytea;
  v_run_work_id uuid;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or (p_expected_cursor is not null and p_expected_cursor<1) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_PIN_INPUT_INVALID';
  end if;
  -- Authorised direct callers use the same outer entry as 0280. Discover
  -- without a job/module/run lock; do not newly admit retained replay/stale
  -- inputs. This read never substitutes for the original locked owner checks.
  select j.* into strict v_job from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select rc.run_id into strict v_run_id from private.bpay_next_run_command rc
    where rc.command_id=v_job.command_id;
  if v_job.status='LEASED' and v_job.phase in ('NEW','PINNING')
     and v_job.lease_nonce is not distinct from p_lease_nonce
     and v_job.owner_epoch is not distinct from p_owner_epoch
     and v_job.lease_until_utc>pg_catalog.clock_timestamp()
     and v_job.cursor_key::bigint is not distinct from p_expected_cursor then
    perform private.weekly_source_pay_query_admit_v2();
    v_query_admitted:=true;
  end if;
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
  if v_job.status='DONE' and v_job.phase='DONE'
     and v_job.job_kind='PREPARE' and v_job.owner_epoch=p_owner_epoch then
    select * into strict v_worker from private.bpay_next_run_worker
      where run_id=v_run_id and candidate_id=v_job.candidate_id;
    if v_worker.status='REVIEW' and v_worker.review_issue_code is not null then
      return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
        'issue_code',v_worker.review_issue_code,
        'work_id',v_worker.review_issue_work_id,'replay',true);
    end if;
  end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.status<>'LEASED' or v_job.job_kind<>'PREPARE'
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',message='BPAY_NEXT_PIN_LEASE_STALE';
  end if;
  if v_job.phase='RESERVE' then
    return pg_catalog.jsonb_build_object('done',true,'phase','RESERVE',
      'replay',true);
  end if;
  if v_job.phase not in ('NEW','PINNING') then
    raise exception using errcode='23514',message='BPAY_NEXT_PIN_PHASE_INVALID';
  end if;
  v_current_cursor:=v_job.cursor_key::bigint;
  if v_current_cursor is distinct from p_expected_cursor then
    return pg_catalog.jsonb_build_object('done',false,'phase',v_job.phase,
      'next_cursor',v_current_cursor,'replay',true);
  end if;
  if not v_query_admitted then
    -- A changed discovery cannot become a fresh mutation after Bank locks.
    -- Roll back and retry the original STEP via the existing 40001 channel;
    -- never acquire the Source key late, fabricate REVIEW or adopt a nonce.
    raise exception using errcode='40001',message='BPAY_NEXT_PIN_ADMISSION_CHANGED';
  end if;
  select * into strict v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  v_worker_id:=v_worker.id;
  select * into v_selection from private.bpay_next_run_selection
    where run_id=v_run_id and candidate_id=v_job.candidate_id
      and selection_no>coalesce(v_current_cursor,0)
    order by selection_no limit 1;
  if not found then
    update private.bpay_next_job set phase='RESERVE',cursor_key=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,'phase','RESERVE',
      'replay',false);
  end if;
  select * into strict v_work from private.bpay_next_work
    where id=v_selection.work_id for share;
  if v_work.candidate_id<>v_job.candidate_id then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PIN_CANDIDATE_MISMATCH';
  end if;
  select * into strict v_choice from private.bpay_next_work_choice
    where run_id=v_run_id and work_id=v_work.id;
  if v_choice.selection_state<>'SEALED' then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SELECTION_COMPONENT_CHOICES_NOT_SEALED';
  end if;
  if v_work.approval_state<>'APPROVED'
     or v_work.current_revision_id is null
     or v_choice.expected_revision_id is null
     or v_choice.expected_revision_id is distinct from v_work.current_revision_id
     or v_work.current_revision_id is distinct from v_work.applied_revision_id then
    -- A newly published revision may be waiting behind this PREPARE command.
    -- End only this worker's non-executable attempt so its later position job
    -- can run. Do not silently omit the work or retain an executable offer.
    update private.bpay_next_run_worker
      set status='REVIEW',review_issue_code=case
        when v_work.approval_state<>'APPROVED' or v_work.current_revision_id is null
          or v_choice.expected_revision_id is null
          then 'SELECTED_WORK_NOT_APPROVED'
        when v_choice.expected_revision_id is distinct from v_work.current_revision_id
          then 'SELECTED_WORK_REVISION_CHANGED'
        else 'SELECTED_WORK_POSITION_PENDING' end,
        review_issue_work_id=v_work.id
      where id=v_worker_id;
    update private.bpay_next_job
      set status='DONE',phase='DONE',cursor_key=null,
          lease_nonce=null,lease_until_utc=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW',
      'issue_code',case
        when v_work.approval_state<>'APPROVED' or v_work.current_revision_id is null
          or v_choice.expected_revision_id is null
          then 'SELECTED_WORK_NOT_APPROVED'
        when v_choice.expected_revision_id is distinct from v_work.current_revision_id
          then 'SELECTED_WORK_REVISION_CHANGED'
        else 'SELECTED_WORK_POSITION_PENDING' end,
      'work_id',v_work.id,'replay',false);
  end if;
  select * into strict v_revision from private.bpay_next_work_revision
    where id=v_choice.expected_revision_id and work_id=v_work.id for share;
  if v_revision.sealed_at_utc is null or v_revision.approved_at_utc is null then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PIN_APPROVAL_NOT_SEALED';
  end if;
  if v_work.work_kind='SOURCE' and v_revision.source_kind in ('SOURCE','PROTECTED') then
    -- Exact point qualification of the approved captured origin. No I3
    -- financial-completion reader, inventory/history rebuild or live price.
    -- Initial authorised TSFIN, committed HEAD and genuine headless direct
    -- NEXT are distinct lanes; none may manufacture the other's authority.
    -- Evaluate the actual classifier as its own statement even for self-bill:
    -- OR/CASE short-circuiting must not hide its permission/integrity errors.
    -- Current domain facts qualify a returned Scope, not immutable provenance
    -- when Source explicitly reports that the current scope is unavailable.
    v_source_classified:=private._candidate_expense_source_family_v1(v_revision.physical_timesheet_id);
    select t.timesheet_id,t.version,t.booking_id,c.client_id,
           family.id as family_id,family.bound_version,
           (t.sheet_scope='WEEKLY'::public.timesheet_scope_enum
            and t.contract_id=v_work.contract_id and c.candidate_id=v_work.candidate_id
            and t.week_ending_date=v_work.week_ending_date
            and t.version=v_revision.physical_timesheet_version
            and t.booking_id=v_work.booking_id and c.client_id is not null
            and v_revision.week_ending_date=v_work.week_ending_date) as root_exact,
           (t.line_type='HOURS'::public.timesheet_line_type_enum and not t.is_adjustment
            and (c.self_bill is true or v_source_classified is true)
            and cw.timesheet_id=t.timesheet_id and not cw.is_adjustment
            and (family.id is null or (family.root_timesheet_id=t.timesheet_id
              and family.root_family_booking_id=t.booking_id
              and family.candidate_id=v_work.candidate_id
              and family.contract_id=v_work.contract_id
              and family.week_ending_date=v_work.week_ending_date
              and family.bound_version>0))) as scope_exact,
           case when v_revision.source_head_id is not null then
             h.id=v_revision.source_event_id and h.root_timesheet_id=t.timesheet_id
             and h.root_timesheet_version=t.version and h.root_family_booking_id=t.booking_id
             and h.candidate_id=v_work.candidate_id and h.contract_id=v_work.contract_id
             and h.week_ending_date=v_work.week_ending_date and h.state in ('COMMITTED_CURRENT','SUPERSEDED')
             and h.committed_at_utc is not null and pg_catalog.isfinite(h.committed_at_utc)
             and h.authority_kind=case when v_revision.source_kind='PROTECTED'
               then 'PROTECTED' else 'LOCKED_FINAL_SOURCE' end
           when v_revision.source_kind='SOURCE' then
             auth.id=v_revision.source_event_id and auth.root_timesheet_id=t.timesheet_id
             and auth.family_booking_id=t.booking_id and auth.timesheet_version=t.version
             and auth.authorised_at_utc is not null and pg_catalog.isfinite(auth.authorised_at_utc)
             and tf.id=v_revision.financial_snapshot_id and tf.timesheet_id=t.timesheet_id
             and tf.timesheet_version=t.version and tf.candidate_id=v_work.candidate_id
             and tf.client_id=c.client_id and tf.authorised_at_utc is not null
             and pg_catalog.isfinite(tf.authorised_at_utc)
           else
             v_revision.source_head_id is null and v_revision.financial_snapshot_id is null
             and receipt.approval_id=v_revision.source_event_id
             and receipt.work_id=v_work.id and receipt.revision_id=v_revision.id
             and receipt.command_id=publication.command_id and receipt.family_id=family.id
             and receipt.agency_sequence=command.agency_sequence
             and receipt.accepted_family_bound_version<=family.bound_version
             and approval.creation_orchestration_run_id=receipt.orchestration_run_id
             and approval.pay_target_family_id=family.id and approval.candidate_id=v_work.candidate_id
             and approval.client_id=c.client_id and approval.contract_id=v_work.contract_id
             and approval.week_ending=v_work.week_ending_date
             and generation.family_id=family.id and generation.lifecycle_state in ('PUBLISHED','SUPERSEDED')
             and generation.published_at_utc is not null
             and generation.result_hash=receipt.publication_request_sha256
             and orchestration.family_id=family.id and orchestration.state='COMPLETE'
             and orchestration.requested_by_user_id=receipt.actor_user_id
             and orchestration.request_fingerprint=receipt.prepared_request_sha256
             and orchestration.after_state_fingerprint=receipt.publication_request_sha256
             and orchestration.completed_at_utc is not null
           end as origin_exact,
           case when v_revision.source_head_id is not null then
             h.state='COMMITTED_CURRENT' and live_auth.current_entitlement_head_id=h.id
           when v_revision.source_kind='SOURCE' then
             live_auth.id=auth.id and live_auth.current_entitlement_head_id is null
             and not exists(select 1 from public.weekly_source_entitlement_heads current_head
               where pg_catalog.btrim(current_head.root_family_booking_id)=pg_catalog.btrim(t.booking_id)
                 and current_head.state='COMMITTED_CURRENT')
           else approval.withdrawn_at_utc is null end as financial_current,
           (publication.candidate_id=v_work.candidate_id
             and publication.revision_no=v_revision.revision_no
             and publication.status='APPLIED' and publication.phase='DONE'
             and command.command_kind='POSITION_APPLY' and command.module_epoch=v_module_epoch
             and command.id=private.bpay_next_source_command_id_v1(v_revision.source_event_id)
             and command.status='COMPLETE' and command.sealed_at_utc is not null
             and command.expected_member_count=command.enrolled_member_count
             and member.member_no is not null) as publication_exact
      into v_source_origin
      from public.timesheets t
      join public.contracts c on c.id=t.contract_id
      -- uq_contract_week makes this one indexed ordinary configured-week
      -- point; a same-contract/week row for another physical root is not scope.
      left join public.contract_weeks cw
        on cw.contract_id=t.contract_id and cw.week_ending_date=t.week_ending_date
          and cw.additional_seq=0
      left join public.weekly_exceptional_pay_target_families family
        on pg_catalog.btrim(family.root_family_booking_id)=pg_catalog.btrim(t.booking_id)
      left join public.weekly_source_entitlement_heads h on h.id=v_revision.source_head_id
      left join public.weekly_source_root_authorisations auth
        on auth.id=v_revision.source_event_id and v_revision.source_head_id is null
          and v_revision.source_kind='SOURCE'
      left join public.weekly_source_root_authorisations live_auth
        on live_auth.root_timesheet_id=t.timesheet_id and live_auth.withdrawn_at_utc is null
      left join public.timesheets_financials tf on tf.id=v_revision.financial_snapshot_id
      left join private.bpay_next_protected_source_receipt receipt
        on receipt.revision_id=v_revision.id and receipt.work_id=v_work.id
      left join public.weekly_exceptional_payment_approvals approval on approval.id=receipt.approval_id
      left join public.weekly_exceptional_pay_generations generation on generation.id=receipt.generation_id
      left join public.weekly_exceptional_orchestration_runs orchestration on orchestration.id=receipt.orchestration_run_id
      left join private.bpay_next_publication publication
        on publication.work_id=v_work.id and publication.revision_id=v_revision.id
      left join private.bpay_next_command command on command.id=publication.command_id
      left join private.bpay_next_command_member member
        on member.command_id=command.id and member.candidate_id=v_work.candidate_id
      where t.timesheet_id=v_revision.physical_timesheet_id;
    if not found or v_source_origin.root_exact is not true
       or v_source_origin.origin_exact is not true or v_source_origin.publication_exact is not true then
      raise exception using errcode='23514',message='BPAY_NEXT_PIN_SOURCE_ORIGIN_INVALID';
    end if;
    -- This is a separate statement AFTER the final existing downstream lock
    -- wait. The shared early admission remains held through the atomic pin.
    -- Source owns the reader/digest; never substitute a local query scan.
    v_query_gate:=private.weekly_source_pay_query_gate_v1(v_revision.physical_timesheet_id);
    if (pg_catalog.jsonb_typeof(v_query_gate)='object'
        and v_query_gate ?& array['ok','code','scope','blocked','query_state_sha256']
        and v_query_gate-array['ok','code','scope','blocked','query_state_sha256']='{}'::jsonb
        and pg_catalog.jsonb_typeof(v_query_gate->'ok')='boolean'
        and pg_catalog.jsonb_typeof(v_query_gate->'code')='string') is not true then
      raise exception using errcode='23514',message='BPAY_NEXT_PIN_QUERY_RESPONSE_INVALID';
    end if;
    v_query_scope:=nullif(v_query_gate->'scope','null'::jsonb);
    if v_query_scope is not null then
      if (v_source_origin.scope_exact is true
          and pg_catalog.jsonb_typeof(v_query_scope)='object'
          and v_query_scope ?& array['root_timesheet_id','family_booking_id','target_family_id',
            'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']
          and v_query_scope-array['root_timesheet_id','family_booking_id','target_family_id',
            'candidate_id','client_id','contract_id','week_ending_date','root_version','family_bound_version']='{}'::jsonb
          and pg_catalog.jsonb_typeof(v_query_scope->'root_timesheet_id')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'family_booking_id')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'candidate_id')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'client_id')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'contract_id')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'week_ending_date')='string'
          and pg_catalog.jsonb_typeof(v_query_scope->'root_version')='string'
          and v_query_scope->>'root_timesheet_id'=v_revision.physical_timesheet_id::text
          and v_query_scope->>'root_version'=v_revision.physical_timesheet_version::text
          and v_query_scope->>'family_booking_id'=v_work.booking_id
          and v_query_scope->>'candidate_id'=v_work.candidate_id::text
          and v_query_scope->>'client_id'=v_source_origin.client_id::text
          and v_query_scope->>'contract_id'=v_work.contract_id::text
          and v_query_scope->>'week_ending_date'=v_work.week_ending_date::text
          and v_query_scope->'target_family_id'=coalesce(pg_catalog.to_jsonb(v_source_origin.family_id::text),'null'::jsonb)
          and v_query_scope->'family_bound_version'=coalesce(pg_catalog.to_jsonb(v_source_origin.bound_version::text),'null'::jsonb)) is not true then
        raise exception using errcode='23514',message='BPAY_NEXT_PIN_QUERY_SCOPE_MISMATCH';
      end if;
    end if;
    v_query_digest:=null;
    if v_query_gate->'ok'='true'::jsonb then
      if (v_query_gate->>'code'='OK' and v_query_scope is not null
          and pg_catalog.jsonb_typeof(v_query_gate->'blocked')='boolean'
          and pg_catalog.jsonb_typeof(v_query_gate->'query_state_sha256')='string'
          and v_query_gate->>'query_state_sha256' ~ '^[0-9a-f]{64}$') is not true then
        raise exception using errcode='23514',message='BPAY_NEXT_PIN_QUERY_RESPONSE_INVALID';
      end if;
      -- The query Scope has no HEAD id. An available/current query root must
      -- never make a superseded or otherwise unmapped financial origin pay.
      -- Conversely an actual defined unavailable current scope retains its
      -- genuine immutable origin and reaches positive-WORK REVIEW below.
      if v_source_origin.financial_current is not true then
        raise exception using errcode='23514',message='BPAY_NEXT_PIN_SOURCE_AUTHORITY_CHANGED';
      end if;
      v_query_digest:=pg_catalog.decode(v_query_gate->>'query_state_sha256','hex');
      v_query_disposition:=case when v_query_gate->'blocked'='true'::jsonb then 'BLOCKED' else 'CLEAR' end;
    elsif ((v_query_gate->>'code'='WEEKLY_SOURCE_PAY_QUERY_SCOPE_UNAVAILABLE' and v_query_scope is null)
        or (v_query_gate->>'code'='WEEKLY_SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE' and v_query_scope is not null))
        and v_query_gate->'blocked'='null'::jsonb and v_query_gate->'query_state_sha256'='null'::jsonb then
      v_query_disposition:='UNAVAILABLE';
    else
      raise exception using errcode='23514',message='BPAY_NEXT_PIN_QUERY_RESPONSE_INVALID';
    end if;
  else
    v_query_disposition:=null;
  end if;
  insert into private.bpay_next_run_work
    (run_worker_id,candidate_id,work_id,captured_revision_id)
    values(v_worker_id,v_job.candidate_id,v_work.id,v_revision.id)
    returning id into v_run_work_id;
  if v_query_disposition is not null then
    insert into private.bpay_next_run_work_query_pin_v2
      (run_work_id,run_worker_id,candidate_id,work_id,captured_revision_id,
       prepare_job_id,module_epoch,gate_version,disposition,scope,query_state_sha256)
      values(v_run_work_id,v_worker_id,v_job.candidate_id,v_work.id,v_revision.id,
        p_job_id,v_module_epoch,'PAY_QUERY_STATE_V3',v_query_disposition,v_query_scope,v_query_digest);
  end if;
  if v_choice.selection_mode='SUBSET' then
    update private.bpay_next_run_worker
      set expected_selected_component_count=
        expected_selected_component_count+v_choice.component_count
      where id=v_worker_id;
  end if;
  update private.bpay_next_job
    set phase='PINNING',cursor_key=v_selection.selection_no::text
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','PINNING',
    'next_cursor',v_selection.selection_no,'work_id',v_work.id,
    'captured_revision_id',v_revision.id,'replay',false);
end
$function$;

alter function private.bpay_next_claim_prepare_job_v1(uuid,integer)
  owner to postgres;
alter function private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint)
  owner to postgres;
revoke all on function
  private.bpay_next_claim_prepare_job_v1(uuid,integer),
  private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint)
  from public,anon,authenticated,service_role;

commit;
