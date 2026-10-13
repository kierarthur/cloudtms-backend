-- WB44 current metadata continuation only. No financial command or revaluation.

\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_workbench_snooze_due_trigger_v1()
returns trigger language plpgsql security definer set search_path=pg_catalog,private
as $function$
begin
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then return new;end if;
  if tg_op='UPDATE' and (new.version,new.until_date,new.active) is not distinct from (old.version,old.until_date,old.active) then return new;end if;
  insert into private.bpay_next_workbench_snooze_due(snooze_id,work_id,case_id,snooze_version,until_date,pending)
    values(new.id,new.work_id,new.case_id,new.version,new.until_date,new.active and new.until_date is not null)
    on conflict(snooze_id) do update set snooze_version=excluded.snooze_version,
      until_date=excluded.until_date,pending=excluded.pending;
  -- The native setter already owns Candidate control. Its real committed
  -- scope enters the same projection barrier; a future expiry is separate.
  if new.work_id is not null then
    perform private.bpay_next_workbench_queue_origin_v1(new.work_id);
  else
    perform private.bpay_next_workbench_refresh_case_v1(new.case_id);
  end if;
  return new;
end
$function$;
drop trigger if exists bpay_next_workbench_snooze_due_trigger on private.bpay_next_snooze;
create trigger bpay_next_workbench_snooze_due_trigger after insert or update of version,until_date,active
  on private.bpay_next_snooze for each row execute function private.bpay_next_workbench_snooze_due_trigger_v1();

create or replace function private.bpay_next_workbench_ensure_metadata_v1(p_candidate_id uuid)
returns private.bpay_next_workbench_candidate_metadata
language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;
begin
  perform private.bpay_next_workbench_require_next_v1();
  insert into private.bpay_next_workbench_candidate_metadata(candidate_id,candidate_sort_name,tms_ref,
    target_pay_channel,target_umbrella_id,target_umbrella_vat_chargeable,target_umbrella_enabled,bank_warning_code)
    select c.id,coalesce(nullif(btrim(c.display_name),''),nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),''),c.id::text),c.tms_ref,
      case when c.pay_method in ('PAYE','UMBRELLA') then c.pay_method end,
      case when c.pay_method='UMBRELLA' then c.umbrella_id end,
      case when c.pay_method='UMBRELLA' then u.vat_chargeable end,
      case when c.pay_method='UMBRELLA' then u.enabled end,
      case when nullif(btrim(case when c.pay_method='UMBRELLA' then u.bank_details_hash else c.bank_details_hash end),'') is null
        then 'BANK_DETAILS_MISSING' end
    from public.candidates c left join public.umbrellas u on u.id=c.umbrella_id where c.id=p_candidate_id
    on conflict(candidate_id) do nothing;
  -- Initialise only the newly added mapping on a retained pre-WB44 row.
  update private.bpay_next_workbench_candidate_metadata m set target_pay_channel=c.pay_method,
    target_umbrella_id=case when c.pay_method='UMBRELLA' then c.umbrella_id end,
    target_umbrella_vat_chargeable=case when c.pay_method='UMBRELLA' then u.vat_chargeable end,
    target_umbrella_enabled=case when c.pay_method='UMBRELLA' then u.enabled end,
    bank_warning_code=case when nullif(btrim(case when c.pay_method='UMBRELLA' then u.bank_details_hash else c.bank_details_hash end),'') is null
      then 'BANK_DETAILS_MISSING' else m.bank_warning_code end
    from public.candidates c left join public.umbrellas u on u.id=c.umbrella_id
    where m.candidate_id=p_candidate_id and c.id=m.candidate_id and m.target_pay_channel is null
      and c.pay_method in ('PAYE','UMBRELLA');
  select m.* into strict v_metadata from private.bpay_next_workbench_candidate_metadata m
    where m.candidate_id=p_candidate_id for update;
  return v_metadata;
end
$function$;

-- Candidate control is preheld by every Candidate-scoped caller.
create or replace function private.bpay_next_workbench_schedule_task_v1(
  p_type text,p_candidate_id uuid,p_work_id uuid,p_umbrella_id uuid,p_bank_changed boolean default false,
  p_control_changed boolean default false,p_previous_pending boolean default null)
returns uuid language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_key text;v_phase text;v_epoch bigint;v_id uuid;v_task private.bpay_next_workbench_task%rowtype;
  v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;v_work private.bpay_next_work%rowtype;
  v_financial bigint;v_wait boolean;v_through_case uuid;v_through_candidate uuid;v_through_work uuid;v_through_group text;v_through_summary text;
begin
  perform private.bpay_next_workbench_require_next_v1();
  select owner_epoch into strict v_epoch from private.bpay_next_module_control where id=1;
  if p_type not in ('METADATA','ORIGIN','UMBRELLA') or p_type is null or p_bank_changed is null or p_control_changed is null then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_TASK_SCOPE_INVALID';end if;
  if p_type='UMBRELLA' then
    if p_candidate_id is not null or p_work_id is not null or p_umbrella_id is null then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_TASK_SCOPE_INVALID';end if;
    v_key:='UMBRELLA:'||p_umbrella_id::text;v_phase:='UMBRELLA_CANDIDATE';
    select candidate_id into v_through_candidate from private.bpay_next_workbench_candidate_metadata
      where target_umbrella_id=p_umbrella_id and target_pay_channel='UMBRELLA' order by candidate_id desc limit 1;
  else
    if p_candidate_id is null or p_umbrella_id is not null or (p_type='ORIGIN')<>(p_work_id is not null) then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_TASK_SCOPE_INVALID';end if;
    insert into private.bpay_next_worker_control(candidate_id) values(p_candidate_id) on conflict do nothing;
    select financial_view_revision into strict v_financial from private.bpay_next_worker_control
      where candidate_id=p_candidate_id for update;
    if p_type='ORIGIN' then
      select task_row.* into strict v_work from private.bpay_next_work task_row where id=p_work_id and candidate_id=p_candidate_id for update;
      v_wait:=private.bpay_next_work_wait_state_v1(p_work_id);
      select wc.case_id into v_through_case from private.bpay_next_work_collection wc
        where wc.work_id=p_work_id and wc.case_id is not null order by wc.case_id desc limit 1;
      v_key:='ORIGIN:'||p_work_id::text;v_phase:='ORIGIN_CASE';
    else
      select group_key into v_through_group from private.bpay_next_workbench_group
        where candidate_id=p_candidate_id order by group_key desc limit 1;
      v_key:='METADATA:'||p_candidate_id::text;v_phase:='METADATA_GROUP';
      select client_scope||':'||pay_channel||':'||section into v_through_summary
        from private.bpay_next_workbench_candidate_summary where candidate_id=p_candidate_id
        order by (client_scope||':'||pay_channel||':'||section) collate "C" desc limit 1;
    end if;
  end if;
  -- Lock task before metadata/revision, always after Candidate/work.
  select task_row.* into v_task from private.bpay_next_workbench_task task_row where task_key=v_key for update;
  if p_candidate_id is not null then
    v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(p_candidate_id);
    if v_task.id is not null and v_task.status<>'DONE' and v_task.module_epoch=v_epoch
      and (p_type='METADATA' and v_task.metadata_version=v_metadata.metadata_version
        or p_type='ORIGIN' and (v_task.expected_revision_id,v_task.expected_applied_revision_id,
          v_task.expected_approval_state,v_task.financial_view_revision,v_task.expected_wait_state)
          is not distinct from (v_work.current_revision_id,v_work.applied_revision_id,
            v_work.approval_state,v_financial,v_wait)) then return v_task.id;end if;
  end if;
  insert into private.bpay_next_workbench_task(task_key,task_type,candidate_id,work_id,umbrella_id,module_epoch,phase,
    through_work_id,through_case_id,through_candidate_id,through_group_key,through_summary_key,financial_view_revision,metadata_version,
    expected_revision_id,expected_applied_revision_id,expected_approval_state,expected_wait_state,bank_warning_changed,control_changed)
    values(v_key,p_type,p_candidate_id,p_work_id,p_umbrella_id,v_epoch,v_phase,v_through_work,v_through_case,v_through_candidate,v_through_group,v_through_summary,
      v_financial,v_metadata.metadata_version,v_work.current_revision_id,v_work.applied_revision_id,
      v_work.approval_state,v_wait,p_bank_changed,p_control_changed)
    on conflict(task_key) do update set generation=private.bpay_next_workbench_task.generation+1,
      module_epoch=excluded.module_epoch,owner_epoch=private.bpay_next_workbench_task.owner_epoch+1,
      status='READY',phase=excluded.phase,cursor_key=null,through_work_id=excluded.through_work_id,
      through_case_id=excluded.through_case_id,through_candidate_id=excluded.through_candidate_id,through_group_key=excluded.through_group_key,
      through_summary_key=excluded.through_summary_key,
      financial_view_revision=excluded.financial_view_revision,metadata_version=excluded.metadata_version,
      expected_revision_id=excluded.expected_revision_id,expected_applied_revision_id=excluded.expected_applied_revision_id,
      expected_approval_state=excluded.expected_approval_state,expected_wait_state=excluded.expected_wait_state,
      bank_warning_changed=excluded.bank_warning_changed or (private.bpay_next_workbench_task.status<>'DONE'
        and private.bpay_next_workbench_task.bank_warning_changed),
      control_changed=excluded.control_changed or(private.bpay_next_workbench_task.status<>'DONE' and private.bpay_next_workbench_task.control_changed),
      step_no=0,rows_visited=0,
      lease_nonce=null,lease_until_utc=null,wake_after_utc=clock_timestamp(),available_at_utc=clock_timestamp(),
      last_error_code=null,updated_at_utc=transaction_timestamp()
    returning id into v_id;
  if p_candidate_id is not null then
    update private.bpay_next_workbench_candidate_metadata set continuation_pending=true,updated_at_utc=transaction_timestamp()
      where candidate_id=p_candidate_id;
  end if;
  update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,
    pending_task_count=pending_task_count+case when v_task.id is null or v_task.status='DONE' then 1 else 0 end,
    dirty_candidate_count=dirty_candidate_count+case when p_candidate_id is not null
      and not coalesce(p_previous_pending,v_metadata.continuation_pending) then 1 else 0 end,
    updated_at_utc=transaction_timestamp() where id=1;
  return v_id;
end
$function$;

create or replace function private.bpay_next_workbench_metadata_changed_v1(p_candidate_id uuid,p_bank_changed boolean default false,p_control_changed boolean default false)
returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;v_warning text;
begin
  perform private.bpay_next_workbench_require_next_v1();
  insert into private.bpay_next_worker_control(candidate_id) values(p_candidate_id) on conflict do nothing;
  perform 1 from private.bpay_next_worker_control where candidate_id=p_candidate_id for update;
  v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(p_candidate_id);
  select case when nullif(btrim(case when c.pay_method='UMBRELLA' then u.bank_details_hash else c.bank_details_hash end),'') is null
    then 'BANK_DETAILS_MISSING' when p_bank_changed then 'BANK_DETAILS_CHANGED' else v_metadata.bank_warning_code end
    into v_warning from public.candidates c left join public.umbrellas u on u.id=c.umbrella_id where c.id=p_candidate_id;
  update private.bpay_next_workbench_candidate_metadata m set
    candidate_sort_name=coalesce(nullif(btrim(c.display_name),''),nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),''),c.id::text),
    tms_ref=c.tms_ref,target_pay_channel=case when c.pay_method in ('PAYE','UMBRELLA') then c.pay_method end,
    target_umbrella_id=case when c.pay_method='UMBRELLA' then c.umbrella_id end,
    target_umbrella_vat_chargeable=case when c.pay_method='UMBRELLA' then u.vat_chargeable end,
    target_umbrella_enabled=case when c.pay_method='UMBRELLA' then u.enabled end,
    bank_warning_code=v_warning,metadata_version=m.metadata_version+1,continuation_pending=true,updated_at_utc=transaction_timestamp()
    from public.candidates c left join public.umbrellas u on u.id=c.umbrella_id
    where m.candidate_id=p_candidate_id and c.id=m.candidate_id;
  perform private.bpay_next_workbench_schedule_task_v1('METADATA',p_candidate_id,null,null,false,p_control_changed,v_metadata.continuation_pending);
end
$function$;

create or replace function private.bpay_next_workbench_candidate_name_trigger_v1()
returns trigger language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_name text;v_old_name text;v_bank_changed boolean;
begin
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then return new;end if;
  v_name:=coalesce(nullif(btrim(new.display_name),''),nullif(btrim(concat_ws(' ',new.first_name,new.last_name)),''),new.id::text);
  v_old_name:=coalesce(nullif(btrim(old.display_name),''),nullif(btrim(concat_ws(' ',old.first_name,old.last_name)),''),old.id::text);
  v_bank_changed:=(new.pay_method,new.umbrella_id) is distinct from (old.pay_method,old.umbrella_id)
    or(new.pay_method is distinct from 'UMBRELLA' and new.bank_details_hash is distinct from old.bank_details_hash);
  if (v_name,new.tms_ref,new.pay_method,new.umbrella_id)
    is not distinct from (v_old_name,old.tms_ref,old.pay_method,old.umbrella_id) and not v_bank_changed then return new;end if;
  perform private.bpay_next_workbench_metadata_changed_v1(new.id,v_bank_changed,
    (new.pay_method,new.umbrella_id) is distinct from (old.pay_method,old.umbrella_id));
  return new;
end
$function$;
drop trigger if exists bpay_next_workbench_candidate_name_trigger on public.candidates;
create trigger bpay_next_workbench_candidate_name_trigger after update of display_name,first_name,last_name,tms_ref,
  pay_method,umbrella_id,bank_details_hash,account_holder,sort_code,account_number on public.candidates
  for each row execute function private.bpay_next_workbench_candidate_name_trigger_v1();

create or replace function private.bpay_next_workbench_umbrella_metadata_trigger_v1()
returns trigger language plpgsql security definer set search_path=pg_catalog,private
as $function$
begin
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then return new;end if;
  if (new.name,new.bank_details_hash,new.vat_chargeable,new.enabled)
    is not distinct from (old.name,old.bank_details_hash,old.vat_chargeable,old.enabled) then return new;end if;
  perform private.bpay_next_workbench_schedule_task_v1('UMBRELLA',null,null,new.id,
    new.bank_details_hash is distinct from old.bank_details_hash,
    (new.vat_chargeable,new.enabled) is distinct from (old.vat_chargeable,old.enabled));
  return new;
end
$function$;
drop trigger if exists bpay_next_workbench_umbrella_metadata_trigger on public.umbrellas;
create trigger bpay_next_workbench_umbrella_metadata_trigger after update of name,bank_details_hash,
  vat_chargeable,enabled,sort_code,account_number on public.umbrellas for each row
  execute function private.bpay_next_workbench_umbrella_metadata_trigger_v1();

create or replace function private.bpay_next_workbench_set_bank_warning_v1(p_candidate_id uuid,p_warning_code text)
returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;
begin
  perform private.bpay_next_workbench_require_next_v1();
  if p_warning_code is not null and p_warning_code not in ('BANK_DETAILS_MISSING','BANK_DETAILS_CHANGED') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_BANK_WARNING_INVALID';end if;
  perform 1 from private.bpay_next_worker_control where candidate_id=p_candidate_id for update;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CANDIDATE_CONTROL_MISSING';end if;
  v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(p_candidate_id);
  if v_metadata.bank_warning_code is not distinct from p_warning_code then return;end if;
  update private.bpay_next_workbench_candidate_metadata set bank_warning_code=p_warning_code,
    metadata_version=metadata_version+1,continuation_pending=true,updated_at_utc=transaction_timestamp() where candidate_id=p_candidate_id;
  perform private.bpay_next_workbench_schedule_task_v1('METADATA',p_candidate_id,null,null,false,false,v_metadata.continuation_pending);
end
$function$;

create or replace function private.bpay_next_workbench_task_result_v1(
  p_task private.bpay_next_workbench_task,p_reveal_lease boolean,p_rows integer,p_progressed boolean,p_stale boolean)
returns jsonb language sql stable security definer set search_path=pg_catalog,private
as $function$
  select jsonb_build_object('task_id',p_task.id,'task_type',p_task.task_type,'status',p_task.status,
    'phase',p_task.phase,'cursor_key',p_task.cursor_key,'generation',p_task.generation::text,
    'owner_epoch',p_task.owner_epoch::text,'step_no',p_task.step_no::text,
    'lease_nonce',case when p_reveal_lease then p_task.lease_nonce end,
    'rows_visited',p_rows::text,'progressed',p_progressed,'stale',p_stale,'error_code',p_task.last_error_code);
$function$;

create or replace function private.bpay_next_workbench_finish_task_v1(p_task_id uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_task private.bpay_next_workbench_task%rowtype;v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;
begin
  perform private.bpay_next_workbench_require_next_v1();
  select task_row.* into strict v_task from private.bpay_next_workbench_task task_row where id=p_task_id for update;
  if v_task.status='DONE' then return;end if;
  update private.bpay_next_workbench_task set status='DONE',phase='DONE',cursor_key=null,
    lease_nonce=null,lease_until_utc=null,last_error_code=null,updated_at_utc=transaction_timestamp() where id=p_task_id;
  if v_task.candidate_id is not null then
    v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(v_task.candidate_id);
    if not exists(select 1 from private.bpay_next_workbench_task where candidate_id=v_task.candidate_id and status<>'DONE') then
      update private.bpay_next_workbench_candidate_metadata set continuation_pending=false,
        adopted_metadata_version=metadata_version,updated_at_utc=transaction_timestamp() where candidate_id=v_task.candidate_id;
    else v_metadata.continuation_pending:=false;end if;
  end if;
  update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,pending_task_count=pending_task_count-1,
    dirty_candidate_count=dirty_candidate_count-case when coalesce(v_metadata.continuation_pending,false) then 1 else 0 end,
    updated_at_utc=transaction_timestamp() where id=1;
end
$function$;

create or replace function private.bpay_next_workbench_queue_origin_v1(p_work_id uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_candidate uuid;
begin
  perform private.bpay_next_workbench_require_next_v1();
  select candidate_id into strict v_candidate from private.bpay_next_work where id=p_work_id;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  perform 1 from private.bpay_next_work where id=p_work_id for update;
  perform private.bpay_next_workbench_schedule_task_v1('ORIGIN',v_candidate,p_work_id,null,false,false);
  perform private.bpay_next_workbench_mark_processing_v1(p_work_id);
end
$function$;

create or replace function private.bpay_next_workbench_claim_task_v1(p_task_id uuid,p_lease_seconds integer)
returns jsonb language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_task private.bpay_next_workbench_task%rowtype;v_epoch bigint;v_now timestamptz;
begin
  perform private.bpay_next_workbench_require_next_v1();
  if p_task_id is null or p_lease_seconds is null or p_lease_seconds not between 1 and 120 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_TASK_INPUT_INVALID';end if;
  select owner_epoch into strict v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  select task_row.* into strict v_task from private.bpay_next_workbench_task task_row where id=p_task_id;
  if v_task.candidate_id is not null then
    perform 1 from private.bpay_next_worker_control where candidate_id=v_task.candidate_id for update;
  end if;
  select task_row.* into strict v_task from private.bpay_next_workbench_task task_row where id=p_task_id for update;
  v_now:=clock_timestamp();
  if v_task.status='DONE' or v_task.module_epoch<>v_epoch
    or(v_task.status='LEASED' and v_task.lease_until_utc>v_now) or v_task.available_at_utc>v_now then
    return private.bpay_next_workbench_task_result_v1(v_task,false,0,false,true);end if;
  update private.bpay_next_workbench_task set status='LEASED',lease_nonce=gen_random_uuid(),
    lease_until_utc=v_now+make_interval(secs=>p_lease_seconds),owner_epoch=owner_epoch+1,
    attempt_count=attempt_count+1,last_error_code=null,updated_at_utc=transaction_timestamp()
    where id=p_task_id returning * into v_task;
  return private.bpay_next_workbench_task_result_v1(v_task,true,0,false,false);
end
$function$;

create or replace function private.bpay_next_workbench_step_task_v1(p_task_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_generation bigint,p_step_no bigint,p_phase text,p_cursor_key text,p_limit integer)
returns jsonb language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_task private.bpay_next_workbench_task%rowtype;v_read private.bpay_next_workbench_task%rowtype;
  v_epoch bigint;v_work_ids uuid[];v_case_ids uuid[];v_candidates uuid[];v_groups text[];v_keys text[];v_snooze_ids uuid[];
  v_id uuid;v_candidate uuid;v_group text;v_last text;v_rows integer:=0;v_more boolean;v_metadata_control_changed boolean;
  v_work private.bpay_next_work%rowtype;v_control private.bpay_next_worker_control%rowtype;
  v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;
  v_before private.bpay_next_workbench_delta[];v_after private.bpay_next_workbench_delta[];v_projection bigint;
begin
  perform private.bpay_next_workbench_require_next_v1();
  if p_task_id is null or p_lease_nonce is null or p_owner_epoch is null or p_generation is null or p_step_no is null
    or p_phase is null or p_limit is null or p_limit not between 1 and 100 or octet_length(p_cursor_key)>64 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_TASK_INPUT_INVALID';end if;
  select owner_epoch into strict v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  select task_row.* into strict v_read from private.bpay_next_workbench_task task_row where id=p_task_id;
  -- Plain discovery precedes all task locks. Revalidation below prevents an old page from advancing.
  if v_read.phase='SEED_WORK' then
    select array_agg(x.id order by x.id) into v_work_ids from(select w.id from private.bpay_next_work w
      where w.approval_state in ('APPROVED','WITHDRAWN') and(w.current_revision_id is not null or w.applied_revision_id is not null)
        and w.id<=v_read.through_work_id and(v_read.cursor_key is null or w.id>v_read.cursor_key::uuid)
      order by w.id limit p_limit) x;
  elsif v_read.phase='SEED_CASE' then
    select array_agg(x.id order by x.id) into v_case_ids from(select c.id from private.bpay_next_finance_case c
      where c.status in ('OPEN','PAUSED') and c.id<=v_read.through_case_id
        and(v_read.cursor_key is null or c.id>v_read.cursor_key::uuid) order by c.id limit p_limit) x;
  elsif v_read.phase in ('SEED_GROUP','METADATA_GROUP') then
    select array_agg(x.group_key order by x.group_key),array_agg(x.work_id) filter(where x.work_id is not null),
      array_agg(x.case_id) filter(where x.case_id is not null) into v_groups,v_work_ids,v_case_ids
      from(select g.group_key,g.work_id,g.case_id from private.bpay_next_workbench_group g
        where(v_read.phase='SEED_GROUP' or g.candidate_id=v_read.candidate_id)
          and g.group_key<=v_read.through_group_key and(v_read.cursor_key is null or g.group_key>v_read.cursor_key)
        order by g.group_key limit p_limit) x;
  elsif v_read.phase='SEED_SNOOZE' then
    select array_agg(x.id order by x.id),array_agg(x.work_id) filter(where x.work_id is not null),
      array_agg(x.case_id) filter(where x.case_id is not null) into v_snooze_ids,v_work_ids,v_case_ids from(
      select s.id,s.work_id,s.case_id from private.bpay_next_snooze s where s.active and s.id<=v_read.through_snooze_id
        and(v_read.cursor_key is null or s.id>v_read.cursor_key::uuid) order by s.id limit p_limit) x;
  elsif v_read.phase='SEED_METADATA' then
    select array_agg(x.candidate_id order by x.candidate_id) into v_candidates from(
      select m.candidate_id from private.bpay_next_workbench_candidate_metadata m
        where m.candidate_id<=v_read.through_candidate_id
          and(v_read.cursor_key is null or m.candidate_id>v_read.cursor_key::uuid)
        order by m.candidate_id limit p_limit) x;
  elsif v_read.phase='METADATA_SUMMARY' then
    select array_agg(x.key order by x.key collate "C") into v_keys from(
      select client_scope||':'||pay_channel||':'||section key
        from private.bpay_next_workbench_candidate_summary where candidate_id=v_read.candidate_id
          and (client_scope||':'||pay_channel||':'||section) collate "C" <= v_read.through_summary_key collate "C"
          and(v_read.cursor_key is null or (client_scope||':'||pay_channel||':'||section) collate "C">v_read.cursor_key collate "C")
        order by (client_scope||':'||pay_channel||':'||section) collate "C" limit p_limit) x;
  elsif v_read.phase='ORIGIN_CASE' then
    select array_agg(x.case_id order by x.case_id) into v_case_ids from(select distinct wc.case_id
      from private.bpay_next_work_collection wc where wc.work_id=v_read.work_id and wc.case_id is not null
        and wc.case_id<=v_read.through_case_id and(v_read.cursor_key is null or wc.case_id>v_read.cursor_key::uuid)
      order by wc.case_id limit p_limit) x;
    v_work_ids:=array[v_read.work_id];
  elsif v_read.phase='ORIGIN_WORK' then v_work_ids:=array[v_read.work_id];
  elsif v_read.phase='UMBRELLA_CANDIDATE' then
    select array_agg(x.candidate_id order by x.candidate_id) into v_candidates from(select m.candidate_id
      from private.bpay_next_workbench_candidate_metadata m where m.target_umbrella_id=v_read.umbrella_id
        and m.target_pay_channel='UMBRELLA' and m.candidate_id<=v_read.through_candidate_id
        and(v_read.cursor_key is null or m.candidate_id>v_read.cursor_key::uuid) order by m.candidate_id limit p_limit) x;
  end if;
  -- Whole page Candidate -> work -> Case preholds, before task/metadata/global revision.
  select array_agg(x.candidate_id order by x.candidate_id) into v_candidates from(
    select candidate_id from private.bpay_next_work where id=any(v_work_ids)
    union select candidate_id from private.bpay_next_finance_case where id=any(v_case_ids)
    union select unnest(v_candidates) union select v_read.candidate_id where v_read.candidate_id is not null) x;
  for v_candidate in select unnest(v_candidates) order by 1 loop
    if v_read.phase='SEED_METADATA' then
      insert into private.bpay_next_worker_control(candidate_id) values(v_candidate) on conflict do nothing;
    end if;
    perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CANDIDATE_CONTROL_MISSING';end if;
  end loop;
  perform 1 from private.bpay_next_work where id=any(v_work_ids) order by id for update;
  perform 1 from private.bpay_next_finance_case where id=any(v_case_ids) order by id for update;
  select task_row.* into strict v_task from private.bpay_next_workbench_task task_row where id=p_task_id for update;
  if v_task.status<>'LEASED' or v_task.module_epoch<>v_epoch or v_task.lease_nonce is distinct from p_lease_nonce
    or v_task.owner_epoch<>p_owner_epoch or v_task.generation<>p_generation or v_task.step_no<>p_step_no
    or(v_task.phase,v_task.cursor_key) is distinct from(p_phase,p_cursor_key)
    or(v_read.generation,v_read.step_no,v_read.phase,v_read.cursor_key)
      is distinct from(v_task.generation,v_task.step_no,v_task.phase,v_task.cursor_key)
    or v_task.lease_until_utc<=clock_timestamp() then
    return private.bpay_next_workbench_task_result_v1(v_task,false,0,false,true);end if;
  if v_task.task_type='ORIGIN' then
    select task_row.* into strict v_work from private.bpay_next_work task_row where id=v_task.work_id;
    select task_row.* into strict v_control from private.bpay_next_worker_control task_row where candidate_id=v_task.candidate_id;
    if(v_task.expected_revision_id,v_task.expected_applied_revision_id,v_task.expected_approval_state,
        v_task.financial_view_revision,v_task.expected_wait_state)
      is distinct from(v_work.current_revision_id,v_work.applied_revision_id,v_work.approval_state,
        v_control.financial_view_revision,private.bpay_next_work_wait_state_v1(v_work.id)) then
      perform private.bpay_next_workbench_schedule_task_v1('ORIGIN',v_task.candidate_id,v_task.work_id,null,false,false);
      select task_row.* into strict v_task from private.bpay_next_workbench_task task_row where id=p_task_id;
      return private.bpay_next_workbench_task_result_v1(v_task,false,0,false,true);
    end if;
  elsif v_task.task_type='METADATA' then
    v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(v_task.candidate_id);
    if v_metadata.metadata_version<>v_task.metadata_version then
      perform private.bpay_next_workbench_schedule_task_v1('METADATA',v_task.candidate_id,null,null,false,v_task.control_changed);
      select task_row.* into strict v_task from private.bpay_next_workbench_task task_row where id=p_task_id;
      return private.bpay_next_workbench_task_result_v1(v_task,false,0,false,true);end if;
  end if;
  begin
    if v_task.task_type='SEED' then
      for v_candidate in select unnest(v_candidates) order by 1 loop
        v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(v_candidate);
        -- Cached current scopes only, including retained rows changed while NEXT was inactive.
        -- Unknown past bank identity cannot create CHANGED; genuine missing remains factual.
        if exists(select 1 from public.candidates c left join public.umbrellas u on u.id=c.umbrella_id
          where c.id=v_candidate and (v_metadata.candidate_sort_name,v_metadata.tms_ref,v_metadata.target_pay_channel,
            v_metadata.target_umbrella_id,v_metadata.target_umbrella_vat_chargeable,v_metadata.target_umbrella_enabled)
          is distinct from (coalesce(nullif(btrim(c.display_name),''),nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),''),c.id::text),
            c.tms_ref,case when c.pay_method in ('PAYE','UMBRELLA') then c.pay_method end,
            case when c.pay_method='UMBRELLA' then c.umbrella_id end,
            case when c.pay_method='UMBRELLA' then u.vat_chargeable end,case when c.pay_method='UMBRELLA' then u.enabled end)
          or c.id=v_candidate and nullif(btrim(case when c.pay_method='UMBRELLA' then u.bank_details_hash else c.bank_details_hash end),'') is null
            and v_metadata.bank_warning_code is distinct from 'BANK_DETAILS_MISSING') then
          select (v_metadata.target_pay_channel,v_metadata.target_umbrella_id,
            v_metadata.target_umbrella_vat_chargeable,v_metadata.target_umbrella_enabled)
            is distinct from (case when c.pay_method in ('PAYE','UMBRELLA') then c.pay_method end,
              case when c.pay_method='UMBRELLA' then c.umbrella_id end,
              case when c.pay_method='UMBRELLA' then u.vat_chargeable end,case when c.pay_method='UMBRELLA' then u.enabled end)
            into v_metadata_control_changed from public.candidates c left join public.umbrellas u on u.id=c.umbrella_id where c.id=v_candidate;
          perform private.bpay_next_workbench_metadata_changed_v1(v_candidate,false,v_metadata_control_changed);
        end if;
      end loop;
    end if;
    if v_task.phase='SEED_WORK' then
      v_rows:=coalesce(cardinality(v_work_ids),0);
      perform private.bpay_next_workbench_refresh_many_v1(v_work_ids,null);
      if v_rows>0 then v_last:=v_work_ids[v_rows]::text;end if;
      if v_rows<p_limit then v_task.phase:='SEED_CASE';v_last:=null;end if;
    elsif v_task.phase='SEED_CASE' then
      v_rows:=coalesce(cardinality(v_case_ids),0);
      perform private.bpay_next_workbench_refresh_many_v1(null,v_case_ids);
      if v_rows>0 then v_last:=v_case_ids[v_rows]::text;end if;
      if v_rows<p_limit then v_task.phase:='SEED_GROUP';v_last:=null;end if;
    elsif v_task.phase='SEED_GROUP' then
      v_rows:=coalesce(cardinality(v_groups),0);
      perform private.bpay_next_workbench_refresh_many_v1(v_work_ids,v_case_ids);
      if v_rows>0 then v_last:=v_groups[v_rows];end if;
      if v_rows<p_limit then v_task.phase:='SEED_SNOOZE';v_last:=null;end if;
    elsif v_task.phase='SEED_SNOOZE' then
      v_rows:=coalesce(cardinality(v_snooze_ids),0);
      insert into private.bpay_next_workbench_snooze_due(snooze_id,work_id,case_id,snooze_version,until_date,pending)
        select s.id,s.work_id,s.case_id,s.version,s.until_date,s.active and s.until_date is not null
          from private.bpay_next_snooze s where s.id=any(v_snooze_ids)
        on conflict(snooze_id) do update set snooze_version=excluded.snooze_version,until_date=excluded.until_date,
          pending=excluded.pending;
      if v_rows>0 then v_last:=v_snooze_ids[v_rows]::text;end if;
      if v_rows<p_limit then v_task.phase:='SEED_METADATA';v_last:=null;end if;
    elsif v_task.phase='SEED_METADATA' then
      v_rows:=coalesce(cardinality(v_candidates),0);
      for v_candidate in select unnest(v_candidates) order by 1 loop
        -- All retained current cache scopes receive a summary adoption page;
        -- zero deltas and inactive-owner edits cannot leave stale display rows.
        perform private.bpay_next_workbench_schedule_task_v1('METADATA',v_candidate,null,null,false,false);
      end loop;
      if v_rows>0 then v_last:=v_candidates[v_rows]::text;end if;
      if v_rows<p_limit then v_task.phase:='SEED_DRAIN';v_last:=null;end if;
    elsif v_task.phase='METADATA_GROUP' then
      v_rows:=coalesce(cardinality(v_groups),0);
      if v_task.control_changed then
        -- Reclassify current native facts; no rate/NET/earnings valuation is called.
        perform private.bpay_next_workbench_refresh_many_v1(v_work_ids,v_case_ids);
      else
        for v_group in select unnest(v_groups) order by 1 loop
          select array_agg(d) into v_before from private.bpay_next_workbench_group_contribution_v1(v_group) d;
          update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,updated_at_utc=transaction_timestamp()
            where id=1 returning workbench_revision into v_projection;
          update private.bpay_next_workbench_item set candidate_display_name=v_metadata.candidate_sort_name,
            candidate_reference=v_metadata.tms_ref,metadata_version=v_metadata.metadata_version,
            projection_revision=v_projection,updated_at_utc=transaction_timestamp() where group_key=v_group;
          update private.bpay_next_workbench_group set metadata_version=v_metadata.metadata_version,
            projection_revision=v_projection,updated_at_utc=transaction_timestamp() where group_key=v_group;
          select array_agg(d) into v_after from private.bpay_next_workbench_group_contribution_v1(v_group) d;
          perform private.bpay_next_workbench_apply_delta_v1(v_before,v_after);
        end loop;
      end if;
      if v_rows>0 then v_last:=v_groups[v_rows];end if;
      if v_rows<p_limit then v_task.phase:='METADATA_SUMMARY';v_last:=null;end if;
    elsif v_task.phase='METADATA_SUMMARY' then
      v_rows:=coalesce(cardinality(v_keys),0);
      update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,
        updated_at_utc=transaction_timestamp() where id=1 returning workbench_revision into v_projection;
      -- Exact <=100 maintained natural keys, including ALL. Never alter a measure/count.
      update private.bpay_next_workbench_candidate_summary s set candidate_sort_name=v_metadata.candidate_sort_name,
        tms_ref=v_metadata.tms_ref,bank_warning_code=v_metadata.bank_warning_code,
        metadata_version=v_metadata.metadata_version,projection_revision=v_projection
        where s.candidate_id=v_task.candidate_id and(s.client_scope||':'||s.pay_channel||':'||s.section)=any(v_keys);
      if v_rows>0 then v_last:=v_keys[v_rows];end if;
      if v_rows<p_limit then perform private.bpay_next_workbench_finish_task_v1(p_task_id);end if;
    elsif v_task.phase='ORIGIN_CASE' then
      v_rows:=coalesce(cardinality(v_case_ids),0);
      for v_id in select unnest(v_case_ids) order by 1 loop
        if not exists(select 1 from private.bpay_next_workbench_group g where g.case_id=v_id
          and g.financial_view_revision=v_task.financial_view_revision and g.complete and not g.dirty) then
          perform private.bpay_next_workbench_refresh_case_v1(v_id);end if;
      end loop;
      if v_rows>0 then v_last:=v_case_ids[v_rows]::text;end if;
      if v_rows<p_limit then v_task.phase:='ORIGIN_WORK';v_last:=null;end if;
    elsif v_task.phase='ORIGIN_WORK' then
      perform private.bpay_next_workbench_refresh_work_core_v1(v_task.work_id);
      v_rows:=1;perform private.bpay_next_workbench_finish_task_v1(p_task_id);
    elsif v_task.phase='UMBRELLA_CANDIDATE' then
      v_rows:=coalesce(cardinality(v_candidates),0);
      for v_candidate in select unnest(v_candidates) order by 1 loop
        -- Membership may have changed since discovery; that writer owns its own task.
        if exists(select 1 from public.candidates c where c.id=v_candidate and c.pay_method='UMBRELLA' and c.umbrella_id=v_task.umbrella_id) then
          perform private.bpay_next_workbench_metadata_changed_v1(v_candidate,v_task.bank_warning_changed,v_task.control_changed);end if;
      end loop;
      if v_rows>0 then v_last:=v_candidates[v_rows]::text;end if;
      if v_rows<p_limit then perform private.bpay_next_workbench_finish_task_v1(p_task_id);end if;
    elsif v_task.phase='SEED_DRAIN' then
      perform 1 from private.bpay_next_workbench_revision where id=1 for update;
      if exists(select 1 from private.bpay_next_workbench_revision where id=1 and pending_task_count=1
        and dirty_candidate_count=0 and dirty_group_count=0) then
        perform private.bpay_next_workbench_finish_task_v1(p_task_id);
        update private.bpay_next_workbench_revision set adoption_state='CURRENT',adoption_revision=adoption_revision+1,
          workbench_revision=workbench_revision+1,updated_at_utc=transaction_timestamp() where id=1;
      end if;
    else raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_TASK_PHASE_INVALID';end if;
    update private.bpay_next_workbench_task set
      status=case when status='DONE' then 'DONE' else 'READY' end,
      phase=case when status='DONE' then 'DONE' else v_task.phase end,
      cursor_key=case when status='DONE' then null else v_last end,
      step_no=step_no+1,rows_visited=rows_visited+v_rows,lease_nonce=null,lease_until_utc=null,
      wake_after_utc=clock_timestamp()+case when v_task.phase='SEED_DRAIN' then interval '1 minute' else interval '0' end,
      available_at_utc=clock_timestamp()+case when v_task.phase='SEED_DRAIN' then interval '1 minute' else interval '0' end,
      last_error_code=null,updated_at_utc=transaction_timestamp() where id=p_task_id returning * into v_task;
    return private.bpay_next_workbench_task_result_v1(v_task,false,v_rows,true,false);
  exception when others then
    -- Inner page writes roll back. Persist only token-free refusal and retain the barrier.
    update private.bpay_next_workbench_task set status='ERROR',lease_nonce=null,lease_until_utc=null,
      wake_after_utc=clock_timestamp()+interval '1 minute',last_error_code='SQLSTATE:'||sqlstate,
      available_at_utc=clock_timestamp()+interval '1 minute',
      updated_at_utc=transaction_timestamp() where id=p_task_id returning * into v_task;
    if v_task.task_type='SEED' then update private.bpay_next_workbench_revision set adoption_state='ERROR',
      workbench_revision=workbench_revision+1,updated_at_utc=transaction_timestamp() where id=1;end if;
    return private.bpay_next_workbench_task_result_v1(v_task,false,0,false,false);
  end;
end
$function$;

create or replace function public.bpay_next_workbench_runtime_service_v1(p_action text,p_task_id uuid,p_args jsonb)
returns jsonb language plpgsql volatile security definer set search_path=pg_catalog,private,public
as $function$
declare v_epoch bigint;v_now timestamptz;v_seed private.bpay_next_workbench_task%rowtype;
  v_task private.bpay_next_workbench_task%rowtype;v_ids uuid[];v_candidates uuid[];v_candidate uuid;
  v_limit integer;v_items jsonb:='[]'::jsonb;v_revision private.bpay_next_workbench_revision%rowtype;
  v_work_ids uuid[];v_case_ids uuid[];v_scope_keys text[];v_work_id uuid;v_case_id uuid;
  v_snooze_ids uuid[];v_snooze_versions bigint[];v_today date;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_RUNTIME_SERVICE_FORBIDDEN';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  if p_action is null or p_args is null or jsonb_typeof(p_args)<>'object' or pg_column_size(p_args)>1024 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_TASK_INPUT_INVALID';end if;
  if p_action='DUE_PAGE' then
    if p_task_id is not null or not(p_args ?& array['limit']) or p_args-array['limit']<>'{}'::jsonb
      or jsonb_typeof(p_args->'limit')<>'number' then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_TASK_INPUT_INVALID';end if;
    v_limit:=(p_args->>'limit')::integer;
    if v_limit not between 1 and 100 then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_TASK_INPUT_INVALID';end if;
    v_today:=(clock_timestamp() at time zone 'Europe/London')::date;
    -- Two real current due indexes; exact Work page, never Candidate/agency history.
    select array_agg(x.scope_key order by x.scope_key) into v_scope_keys from(
      select scope_key from(select case when s.work_id is not null then 'WORK:'||s.work_id::text else 'CASE:'||s.case_id::text end scope_key
        from private.bpay_next_workbench_snooze_due s
        where s.pending and s.until_date<v_today
        order by s.until_date,s.snooze_id limit v_limit) snooze_due
      union select scope_key from(select i.group_key scope_key from private.bpay_next_workbench_item i
        where i.reason_code='FUTURE_WEEK' and i.eligible_from_date<=v_today
        order by i.eligible_from_date,i.id limit v_limit) week_due
      order by scope_key limit v_limit) x;
    select array_agg(substring(k from 6)::uuid) filter(where k like 'WORK:%'),
      array_agg(substring(k from 6)::uuid) filter(where k like 'CASE:%') into v_work_ids,v_case_ids from unnest(v_scope_keys) k;
    for v_candidate in select candidate_id from(
      select w.candidate_id from private.bpay_next_work w where w.id=any(v_work_ids)
      union select c.candidate_id from private.bpay_next_finance_case c where c.id=any(v_case_ids)) x order by candidate_id loop
      perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CANDIDATE_CONTROL_MISSING';end if;
    end loop;
    perform 1 from private.bpay_next_work where id=any(v_work_ids) order by id for update;
    perform 1 from private.bpay_next_finance_case where id=any(v_case_ids) order by id for update;
    -- Version-bound bounded receipt; do not acknowledge any unvisited snooze.
    select array_agg(x.id order by x.id),array_agg(x.version order by x.id) into v_snooze_ids,v_snooze_versions
      from(select s.snooze_id id,s.snooze_version version from private.bpay_next_workbench_snooze_due s
        where s.pending and s.until_date<v_today and(s.work_id=any(v_work_ids) or s.case_id=any(v_case_ids))
        order by s.until_date,s.snooze_id limit v_limit) x;
    perform 1 from private.bpay_next_workbench_snooze_due where snooze_id=any(v_snooze_ids) order by snooze_id for update;
    for v_work_id in select unnest(v_work_ids) order by 1 loop
      perform private.bpay_next_workbench_queue_origin_v1(v_work_id);
    end loop;
    for v_case_id in select unnest(v_case_ids) order by 1 loop
      perform private.bpay_next_workbench_refresh_case_v1(v_case_id);
    end loop;
    update private.bpay_next_workbench_snooze_due s set pending=false
      from unnest(v_snooze_ids,v_snooze_versions) x(id,version) where s.snooze_id=x.id and s.snooze_version=x.version
        and s.pending and s.until_date<v_today;
    return jsonb_build_object('rows_visited',coalesce(cardinality(v_scope_keys),0)::text);
  elsif p_action='WAKE_PAGE' then
    if p_task_id is not null or not(p_args ?& array['limit']) or p_args-array['limit']<>'{}'::jsonb
      or jsonb_typeof(p_args->'limit')<>'number' then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_TASK_INPUT_INVALID';end if;
    v_limit:=(p_args->>'limit')::integer;
    if v_limit not between 1 and 100 then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_TASK_INPUT_INVALID';end if;
    select task_row.* into strict v_seed from private.bpay_next_workbench_task task_row where task_key='SEED';
    if not v_seed.seed_fenced or v_seed.module_epoch<>v_epoch then
      select task_row.* into strict v_seed from private.bpay_next_workbench_task task_row where task_key='SEED' for update;
      if not v_seed.seed_fenced or v_seed.module_epoch<>v_epoch then
        update private.bpay_next_workbench_task set generation=generation+case when seed_fenced then 1 else 0 end,
          module_epoch=v_epoch,owner_epoch=owner_epoch+1,status='READY',phase='SEED_WORK',cursor_key=null,
          through_work_id=(select w.id from private.bpay_next_work w where w.approval_state in ('APPROVED','WITHDRAWN')
            and(w.current_revision_id is not null or w.applied_revision_id is not null) order by w.id desc limit 1),
          through_case_id=(select c.id from private.bpay_next_finance_case c where c.status in ('OPEN','PAUSED') order by c.id desc limit 1),
          through_group_key=(select g.group_key from private.bpay_next_workbench_group g order by g.group_key desc limit 1),
          through_candidate_id=(select m.candidate_id from private.bpay_next_workbench_candidate_metadata m
            order by m.candidate_id desc limit 1),
          through_snooze_id=(select s.id from private.bpay_next_snooze s where s.active order by s.id desc limit 1),
          seed_fenced=true,step_no=0,rows_visited=0,lease_nonce=null,lease_until_utc=null,
          wake_after_utc=clock_timestamp()+interval '1 minute',available_at_utc=clock_timestamp(),last_error_code=null,
          updated_at_utc=transaction_timestamp() where id=v_seed.id;
        update private.bpay_next_workbench_revision set adoption_state='SEEDING',workbench_revision=workbench_revision+1,
          pending_task_count=pending_task_count+case when v_seed.status='DONE' then 1 else 0 end,
          updated_at_utc=transaction_timestamp() where id=1;
        -- Bootstrap returns here: never hold Seed task then acquire a Candidate.
        v_items:=jsonb_build_array(jsonb_build_object('task_id',v_seed.id));
      end if;
    else
      v_now:=clock_timestamp();
      select array_agg(x.id),array_agg(x.candidate_id) filter(where x.candidate_id is not null)
        into v_ids,v_candidates from(select t.id,t.candidate_id from private.bpay_next_workbench_task t
          where t.wake_after_utc<=v_now and t.available_at_utc<=v_now
            and(t.status in ('READY','ERROR') or t.status='LEASED' and t.lease_until_utc<=v_now)
          order by t.wake_after_utc,t.id limit v_limit) x;
      for v_candidate in select distinct unnest(v_candidates) order by 1 loop
        perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
      end loop;
      for v_task in select t.* from private.bpay_next_workbench_task t where t.id=any(v_ids)
        and t.wake_after_utc<=v_now and t.available_at_utc<=v_now
        and(t.status in ('READY','ERROR') or t.status='LEASED' and t.lease_until_utc<=v_now)
        order by t.wake_after_utc,t.id limit v_limit for update skip locked loop
        if v_task.module_epoch<>v_epoch then
          perform private.bpay_next_workbench_schedule_task_v1(v_task.task_type,v_task.candidate_id,v_task.work_id,
            v_task.umbrella_id,v_task.bank_warning_changed,v_task.control_changed);
        end if;
        update private.bpay_next_workbench_task set wake_after_utc=v_now+interval '1 minute' where id=v_task.id;
        v_items:=v_items||jsonb_build_array(jsonb_build_object('task_id',v_task.id));
      end loop;
    end if;
    select task_row.* into strict v_revision from private.bpay_next_workbench_revision task_row where id=1;
    return jsonb_build_object('items',v_items,'adoption_state',v_revision.adoption_state,
      'adoption_revision',v_revision.adoption_revision::text,'pending_task_count',v_revision.pending_task_count::text);
  elsif p_action='CLAIM' then
    if p_task_id is null or not(p_args ?& array['lease_seconds']) or p_args-array['lease_seconds']<>'{}'::jsonb
      or jsonb_typeof(p_args->'lease_seconds')<>'number' then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_TASK_INPUT_INVALID';end if;
    return private.bpay_next_workbench_claim_task_v1(p_task_id,(p_args->>'lease_seconds')::integer);
  elsif p_action='STEP' then
    if p_task_id is null or not(p_args ?& array['lease_nonce','owner_epoch','generation','step_no','phase','cursor_key','limit'])
      or p_args-array['lease_nonce','owner_epoch','generation','step_no','phase','cursor_key','limit']<>'{}'::jsonb
      or jsonb_typeof(p_args->'lease_nonce')<>'string' or jsonb_typeof(p_args->'owner_epoch')<>'string'
      or jsonb_typeof(p_args->'generation')<>'string' or jsonb_typeof(p_args->'step_no')<>'string'
      or jsonb_typeof(p_args->'phase')<>'string' or jsonb_typeof(p_args->'cursor_key') not in ('string','null')
      or jsonb_typeof(p_args->'limit')<>'number'
      or(p_args->>'owner_epoch')!~'^[1-9][0-9]{0,18}$' or(p_args->>'generation')!~'^[1-9][0-9]{0,18}$'
      or(p_args->>'step_no')!~'^(0|[1-9][0-9]{0,18})$' then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_TASK_INPUT_INVALID';end if;
    return private.bpay_next_workbench_step_task_v1(p_task_id,(p_args->>'lease_nonce')::uuid,
      (p_args->>'owner_epoch')::bigint,(p_args->>'generation')::bigint,(p_args->>'step_no')::bigint,
      p_args->>'phase',p_args->>'cursor_key',(p_args->>'limit')::integer);
  end if;
  raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_TASK_INPUT_INVALID';
end
$function$;

alter function private.bpay_next_workbench_snooze_due_trigger_v1() owner to postgres;
revoke all on function private.bpay_next_workbench_snooze_due_trigger_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_ensure_metadata_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_ensure_metadata_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_schedule_task_v1(text,uuid,uuid,uuid,boolean,boolean,boolean) owner to postgres;
revoke all on function private.bpay_next_workbench_schedule_task_v1(text,uuid,uuid,uuid,boolean,boolean,boolean) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_metadata_changed_v1(uuid,boolean,boolean) owner to postgres;
revoke all on function private.bpay_next_workbench_metadata_changed_v1(uuid,boolean,boolean) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_candidate_name_trigger_v1() owner to postgres;
revoke all on function private.bpay_next_workbench_candidate_name_trigger_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_umbrella_metadata_trigger_v1() owner to postgres;
revoke all on function private.bpay_next_workbench_umbrella_metadata_trigger_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_set_bank_warning_v1(uuid,text) owner to postgres;
revoke all on function private.bpay_next_workbench_set_bank_warning_v1(uuid,text) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_task_result_v1(private.bpay_next_workbench_task,boolean,integer,boolean,boolean) owner to postgres;
revoke all on function private.bpay_next_workbench_task_result_v1(private.bpay_next_workbench_task,boolean,integer,boolean,boolean) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_finish_task_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_finish_task_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_queue_origin_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_queue_origin_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_claim_task_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_workbench_claim_task_v1(uuid,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_step_task_v1(uuid,uuid,bigint,bigint,bigint,text,text,integer) owner to postgres;
revoke all on function private.bpay_next_workbench_step_task_v1(uuid,uuid,bigint,bigint,bigint,text,text,integer) from public,anon,authenticated,service_role;
alter function public.bpay_next_workbench_runtime_service_v1(text,uuid,jsonb) owner to postgres;
revoke all on function public.bpay_next_workbench_runtime_service_v1(text,uuid,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_workbench_runtime_service_v1(text,uuid,jsonb) to service_role;
notify pgrst,'reload schema';
commit;
