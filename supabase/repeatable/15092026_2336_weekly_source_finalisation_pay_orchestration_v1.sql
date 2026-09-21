-- Repeatable CloudTMS authority: weekly_source_finalisation_pay_orchestration_v1
-- Durable, receipt-led orchestration between immutable source finalisation and
-- the existing ordinary Weekly Timesheet/TSFIN projection.  It never calls or
-- mutates Workbench, Banking Pay, Draft, invoice or provider owners.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_finalisation_pay_context_v1(
  p_final_revision_id uuid,
  p_root_timesheet_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_revision public.weekly_source_final_revisions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_scope public.weekly_source_report_scopes%rowtype;
  v_upload public.weekly_source_uploads%rowtype;
  v_profile public.weekly_source_format_profiles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_manifest public.weekly_source_client_manifests%rowtype;
  v_client_id uuid;
  v_client_count integer;
  v_profile_count integer;
  v_source_profile_kind text;
  v_source_mode text;
  v_source_units jsonb;
  v_segments jsonb;
  v_schedule jsonb;
  v_expenses jsonb;
  v_source_unit_hash bytea;
  v_source_expense_hash bytea;
  v_active_segment_hash bytea;
  v_rate_refs jsonb;
  v_context jsonb;
  v_context_hash bytea;
begin
  if p_final_revision_id is null or p_root_timesheet_id is null then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_CONTEXT_INVALID'
      using errcode='22023';
  end if;
  select * into v_revision
  from public.weekly_source_final_revisions revision
  where revision.id=p_final_revision_id;
  if not found or v_revision.state<>'CURRENT'
     or v_revision.reason<>'INITIAL_FINALISATION' then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_REVISION_INVALID'
      using errcode='55000';
  end if;
  select * into strict v_cycle
  from public.weekly_source_cycles cycle where cycle.id=v_revision.source_cycle_id;
  select * into strict v_upload
  from public.weekly_source_uploads upload where upload.id=v_revision.upload_id;
  select * into strict v_profile
  from public.weekly_source_format_profiles profile
  where profile.id=v_upload.source_format_profile_id;
  select * into strict v_group
  from public.weekly_source_groups source_group
  where source_group.id=v_cycle.source_group_id;
  if v_revision.authority_scope_kind='CYCLE' then
    if v_cycle.current_final_revision_id is distinct from v_revision.id
       or v_upload.report_scope_id is not null then
      raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_REVISION_STALE'
        using errcode='40001';
    end if;
  else
    select * into v_scope
    from public.weekly_source_report_scopes report_scope
    where report_scope.id=v_revision.report_scope_id;
    if not found or v_scope.current_final_revision_id is distinct from v_revision.id
       or v_scope.source_cycle_id is distinct from v_cycle.id
       or v_upload.report_scope_id is distinct from v_scope.id then
      raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_REVISION_STALE'
        using errcode='40001';
    end if;
  end if;

  v_source_profile_kind:=v_profile.final_authority_kind;
  v_source_mode:=case when v_source_profile_kind='NHSP_TRUST_BACKING_REPORT'
    then 'NHSP_WEEKLY' else 'HEALTHROSTER_WEEKLY' end;
  if v_source_profile_kind not in (
       'GENERIC_COMPLETE_SNAPSHOT','NHSP_TRUST_BACKING_REPORT','HEALTHROSTER_ACTUAL_ROWS'
     ) or (v_group.source_family='NHSP') is distinct from (v_source_mode='NHSP_WEEKLY') then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_PROFILE_INVALID'
      using errcode='55000';
  end if;

  select pg_catalog.count(distinct movement.actual_client_id),
         pg_catalog.count(distinct movement.source_profile_kind),
         (pg_catalog.array_agg(movement.actual_client_id order by movement.id))[1]
    into v_client_count,v_profile_count,v_client_id
  from public.weekly_source_billing_movements movement
  where movement.final_revision_id=v_revision.id
    and movement.invoice_timesheet_id=p_root_timesheet_id;
  if v_client_count<>1 or v_profile_count<>1 or v_client_id is null
     or exists(
       select 1 from public.weekly_source_billing_movements movement
       where movement.final_revision_id=v_revision.id
         and movement.invoice_timesheet_id=p_root_timesheet_id
         and movement.source_profile_kind is distinct from v_source_profile_kind
     ) then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_ROOT_SCOPE_INVALID'
      using errcode='55000';
  end if;
  select * into v_manifest
  from public.weekly_source_client_manifests manifest
  where manifest.final_revision_id=v_revision.id and manifest.client_id=v_client_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_MANIFEST_MISSING'
      using errcode='55000';
  end if;
  if not exists(
    select 1 from public.timesheets timesheet_row
    where timesheet_row.timesheet_id=p_root_timesheet_id
      and timesheet_row.is_current
      and timesheet_row.sheet_scope='WEEKLY'::public.timesheet_scope_enum
      and timesheet_row.line_type='HOURS'::public.timesheet_line_type_enum
      and not timesheet_row.is_adjustment
      and timesheet_row.revoked_at is null
      and timesheet_row.archived_at_utc is null
  ) then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_ROOT_INVALID'
      using errcode='55000';
  end if;

  v_source_units:=private.weekly_source_ordinary_projection_source_units_v1(
    v_revision.id,p_root_timesheet_id
  );
  v_segments:=private.weekly_source_ordinary_projection_current_segments_v1(
    p_root_timesheet_id,v_revision.id
  );
  v_schedule:=private.weekly_source_ordinary_projection_actual_schedule_v1(v_segments);
  v_expenses:=private.weekly_source_ordinary_projection_current_expenses_v1(
    p_root_timesheet_id,v_revision.id
  );
  v_source_unit_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ORDINARY_UNIT_MANIFEST_V1',v_source_units
  );
  v_source_expense_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ORDINARY_SOURCE_EXPENSE_MANIFEST_V1',v_expenses
  );
  v_active_segment_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_ORDINARY_ACTIVE_SEGMENTS_V1',v_segments
  );
  v_rate_refs:=pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_FINAL_AUTHORITY_RATE_SOURCE_V1',
    'source_mode',v_source_mode,'root_timesheet_id',p_root_timesheet_id,
    'final_revision_id',v_revision.id,
    'final_manifest_hash',pg_catalog.encode(v_revision.manifest_hash,'hex'),
    'final_policy_fingerprint',pg_catalog.encode(v_revision.policy_fingerprint,'hex'),
    'client_manifest_hash',pg_catalog.encode(v_manifest.manifest_hash,'hex'),
    'source_unit_manifest_hash',pg_catalog.encode(v_source_unit_hash,'hex'),
    'source_expense_manifest_hash',pg_catalog.encode(v_source_expense_hash,'hex'),
    'active_segment_manifest_hash',pg_catalog.encode(v_active_segment_hash,'hex')
  );
  v_context:=pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_FINALISATION_PAY_ROOT_CONTEXT_V1',
    'final_revision_id',v_revision.id,
    'source_cycle_id',v_cycle.id,
    'client_manifest_id',v_manifest.id,
    'client_id',v_client_id,
    'root_timesheet_id',p_root_timesheet_id,
    'source_profile_kind',v_source_profile_kind,
    'source_mode',v_source_mode,
    'expected_segments',v_segments,
    'expected_actual_schedule',v_schedule,
    'expected_source_expenses',v_expenses,
    'expected_rate_source_refs',v_rate_refs
  );
  v_context_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_FINALISATION_PAY_ROOT_CONTEXT_V1',v_context
  );
  return v_context||pg_catalog.jsonb_build_object(
    'prepared_context_hash',pg_catalog.encode(v_context_hash,'hex')
  );
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_CONTEXT_SCOPE_INVALID'
      using errcode='55000';
end;
$function$;

create or replace function private.weekly_source_finalisation_pay_task_json_v1(
  p_task public.weekly_source_finalisation_pay_tasks
) returns jsonb
language sql stable
set search_path to 'public','pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_build_object(
    'task_id',p_task.id,
    'task_ordinal',p_task.task_ordinal,
    'client_manifest_id',p_task.client_manifest_id,
    'root_timesheet_id',p_task.root_timesheet_id,
    'projection_idempotency_key',p_task.projection_idempotency_key,
    'prepared_context_hash',pg_catalog.encode(p_task.prepared_context_hash,'hex'),
    'root_context',p_task.prepared_context_json,
    'state',p_task.state,
    'attempt_count',p_task.attempt_count,
    'version',p_task.version,
    'projection_receipt_id',p_task.projection_receipt_id,
    'requires_recovery',p_task.state in ('SUBMISSION_STARTED','RECOVERY_REQUIRED'),
    'action_required',p_task.state='FAILED'
  );
$function$;

create or replace function private.weekly_source_finalisation_pay_run_refresh_v1(
  p_run_id uuid
) returns public.weekly_source_finalisation_pay_runs
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_run public.weekly_source_finalisation_pay_runs%rowtype;
  v_total integer;
  v_terminal integer;
  v_action integer;
  v_recovery integer;
  v_started integer;
  v_state text;
begin
  select * into v_run
  from public.weekly_source_finalisation_pay_runs run_row
  where run_row.id=p_run_id for update;
  if not found then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_RUN_NOT_FOUND'
      using errcode='22023';
  end if;
  select pg_catalog.count(*)::integer,
         pg_catalog.count(*) filter(where task.state in (
           'PREPARED_FOR_AUTHORISATION','PROPOSED','NO_OP_FIRST_NEGATIVE',
           'TARGET_MANAGED_SUPPRESSED','FAILED'
         ))::integer,
         pg_catalog.count(*) filter(where task.state='FAILED')::integer,
         pg_catalog.count(*) filter(where task.state in (
           'SUBMISSION_STARTED','RECOVERY_REQUIRED'
         ))::integer,
         pg_catalog.count(*) filter(where task.state='SUBMISSION_STARTED')::integer
    into v_total,v_terminal,v_action,v_recovery,v_started
  from public.weekly_source_finalisation_pay_tasks task
  where task.run_id=v_run.id;
  if v_total<>v_run.task_count then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_TASK_MANIFEST_CHANGED'
      using errcode='55000';
  end if;
  v_state:=case
    when v_recovery>0 then 'RECOVERY_REQUIRED'
    when v_action>0 then 'ACTION_REQUIRED'
    when v_terminal=v_total then 'COMPLETE'
    when v_started>0 then 'RUNNING'
    else 'READY'
  end;
  update public.weekly_source_finalisation_pay_runs run_row
  set terminal_task_count=v_terminal,
      action_required_task_count=v_action,
      state=v_state,
      completed_at_utc=case when v_state='COMPLETE'
        then coalesce(run_row.completed_at_utc,pg_catalog.transaction_timestamp())
        else null end
  where run_row.id=v_run.id
  returning * into v_run;
  return v_run;
end;
$function$;

create or replace function private.weekly_source_finalisation_pay_run_json_v1(
  p_run public.weekly_source_finalisation_pay_runs
) returns jsonb
language sql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  select pg_catalog.jsonb_build_object(
    'ok',p_run.state in ('READY','RUNNING','COMPLETE'),
    'run_id',p_run.id,
    'final_revision_id',p_run.final_revision_id,
    'source_cycle_id',p_run.source_cycle_id,
    'state',p_run.state,
    'task_count',p_run.task_count,
    'terminal_task_count',p_run.terminal_task_count,
    'action_required_task_count',p_run.action_required_task_count,
    'task_manifest_hash',pg_catalog.encode(p_run.task_manifest_hash,'hex'),
    'run_hash',pg_catalog.encode(p_run.run_hash,'hex'),
    'tasks',coalesce((
      select pg_catalog.jsonb_agg(
        private.weekly_source_finalisation_pay_task_json_v1(task)
        order by task.task_ordinal
      )
      from public.weekly_source_finalisation_pay_tasks task
      where task.run_id=p_run.id
    ),'[]'::jsonb)
  );
$function$;

create or replace function public.weekly_source_finalisation_pay_open_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_expected_keys text[]:=array['actor_user_id','final_revision_id','schema_version'];
  v_actual_keys text[];
  v_actor uuid;
  v_revision_id uuid;
  v_revision public.weekly_source_final_revisions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_group public.weekly_source_groups%rowtype;
  v_run public.weekly_source_finalisation_pay_runs%rowtype;
  v_contexts jsonb;
  v_task_manifest_hash bytea;
  v_run_hash bytea;
  v_task_count integer;
  v_context record;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_OPEN_INVALID' using errcode='22023';
  end if;
  select pg_catalog.array_agg(key order by key) into v_actual_keys
  from pg_catalog.jsonb_object_keys(p_request) key;
  select pg_catalog.array_agg(key order by key) into v_expected_keys
  from pg_catalog.unnest(v_expected_keys) key;
  if v_actual_keys is distinct from v_expected_keys
     or p_request->>'schema_version'<>'WEEKLY_SOURCE_FINALISATION_PAY_OPEN_V1' then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_OPEN_CONTRACT_INVALID'
      using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_revision_id:=(p_request->>'final_revision_id')::uuid;
  exception when invalid_text_representation then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_OPEN_INVALID' using errcode='22023';
  end;
  perform 1 from public.tms_users actor
  where actor.id=v_actor and coalesce(actor.is_active,false);
  if v_actor is null or v_revision_id is null or not found then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_ACTOR_INVALID' using errcode='42501';
  end if;

  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
    'WEEKLY_SOURCE_FINALISATION_PAY_RUN|'||v_revision_id::text,0
  ));
  select * into v_revision
  from public.weekly_source_final_revisions revision
  where revision.id=v_revision_id for share;
  if not found or v_revision.state<>'CURRENT'
     or v_revision.reason<>'INITIAL_FINALISATION' then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_REVISION_INVALID'
      using errcode='55000';
  end if;
  select * into strict v_cycle
  from public.weekly_source_cycles cycle where cycle.id=v_revision.source_cycle_id;
  select * into strict v_group
  from public.weekly_source_groups source_group where source_group.id=v_cycle.source_group_id;
  for v_context in
    select distinct movement.actual_client_id as client_id
    from public.weekly_source_billing_movements movement
    where movement.final_revision_id=v_revision.id
    order by movement.actual_client_id
  loop
    perform private.weekly_source_office_authority_v1(
      v_actor,'FINALISE_WEEK',v_group.id,v_context.client_id,
      v_cycle.finalisation_week_ending
    );
  end loop;

  select * into v_run
  from public.weekly_source_finalisation_pay_runs run_row
  where run_row.final_revision_id=v_revision.id;
  if found then
    if v_run.requested_by_user_id is distinct from v_actor
       or v_run.final_revision_manifest_hash is distinct from v_revision.manifest_hash then
      raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_RUN_IDENTITY_CHANGED'
        using errcode='40001';
    end if;
    v_run:=private.weekly_source_finalisation_pay_run_refresh_v1(v_run.id);
    return private.weekly_source_finalisation_pay_run_json_v1(v_run)
      ||pg_catalog.jsonb_build_object('idempotent_replay',true);
  end if;

  select coalesce(pg_catalog.jsonb_agg(context_value order by root_timesheet_id),'[]'::jsonb)
    into v_contexts
  from (
    select roots.root_timesheet_id,
           private.weekly_source_finalisation_pay_context_v1(
             v_revision.id,roots.root_timesheet_id
           ) as context_value
    from (
      select distinct movement.invoice_timesheet_id as root_timesheet_id
      from public.weekly_source_billing_movements movement
      where movement.final_revision_id=v_revision.id
        and movement.invoice_timesheet_id is not null
    ) roots
  ) prepared;
  v_task_count:=pg_catalog.jsonb_array_length(v_contexts);
  v_task_manifest_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_FINALISATION_PAY_TASK_MANIFEST_V1',v_contexts
  );
  v_run_hash:=private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_FINALISATION_PAY_RUN_V1',
    pg_catalog.jsonb_build_object(
      'final_revision_id',v_revision.id,
      'source_cycle_id',v_cycle.id,
      'requested_by_user_id',v_actor,
      'final_revision_manifest_hash',pg_catalog.encode(v_revision.manifest_hash,'hex'),
      'task_manifest_hash',pg_catalog.encode(v_task_manifest_hash,'hex'),
      'task_count',v_task_count
    )
  );
  insert into public.weekly_source_finalisation_pay_runs(
    final_revision_id,source_cycle_id,requested_by_user_id,orchestration_key,
    final_revision_manifest_hash,task_manifest_hash,task_count,
    terminal_task_count,action_required_task_count,state,run_hash,completed_at_utc
  ) values (
    v_revision.id,v_cycle.id,v_actor,'weekly-source-finalisation-pay:'||v_revision.id::text,
    v_revision.manifest_hash,v_task_manifest_hash,v_task_count,0,0,
    case when v_task_count=0 then 'COMPLETE' else 'READY' end,v_run_hash,
    case when v_task_count=0 then pg_catalog.transaction_timestamp() else null end
  ) returning * into v_run;
  -- S9 / WB-016: every task carries the Timesheet FAMILY identity and the
  -- version it was opened against, so a retry, recovery or delayed release
  -- resolves through the family instead of trusting a stored physical id.  The
  -- run-level uniqueness is now on the family, not on root_timesheet_id.
  insert into public.weekly_source_finalisation_pay_tasks(
    run_id,task_ordinal,client_manifest_id,root_timesheet_id,
    root_family_booking_id,root_timesheet_version,
    projection_idempotency_key,prepared_context_json,prepared_context_hash,state
  )
  select v_run.id,entry.ordinality::integer,
         (entry.value->>'client_manifest_id')::uuid,
         (entry.value->>'root_timesheet_id')::uuid,
         root_timesheet.booking_id,
         root_timesheet.version,
         'weekly-source-finalisation-pay:'||v_revision.id::text||':'||
           (entry.value->>'root_timesheet_id'),
         entry.value-(array['prepared_context_hash']::text[]),
         pg_catalog.decode(entry.value->>'prepared_context_hash','hex'),'READY'
  from pg_catalog.jsonb_array_elements(v_contexts) with ordinality entry(value,ordinality)
  join public.timesheets root_timesheet
    on root_timesheet.timesheet_id=(entry.value->>'root_timesheet_id')::uuid;
  -- The join above would silently drop a context whose root Timesheet is not
  -- present, so the manifest count is asserted rather than assumed.
  if (select pg_catalog.count(*)::integer
      from public.weekly_source_finalisation_pay_tasks task
      where task.run_id=v_run.id)<>v_task_count then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_TASK_MANIFEST_CHANGED'
      using errcode='55000';
  end if;
  return private.weekly_source_finalisation_pay_run_json_v1(v_run)
    ||pg_catalog.jsonb_build_object('idempotent_replay',false);
exception
  when no_data_found or too_many_rows then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_OPEN_SCOPE_INVALID'
      using errcode='55000';
end;
$function$;

create or replace function public.weekly_source_finalisation_pay_task_start_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_expected_keys text[]:=array[
    'actor_user_id','expected_context_hash','expected_task_version',
    'run_id','schema_version','task_id'
  ];
  v_actual_keys text[];
  v_actor uuid;
  v_run_id uuid;
  v_task_id uuid;
  v_version bigint;
  v_context_hash bytea;
  v_run public.weekly_source_finalisation_pay_runs%rowtype;
  v_task public.weekly_source_finalisation_pay_tasks%rowtype;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_TASK_START_INVALID' using errcode='22023';
  end if;
  select pg_catalog.array_agg(key order by key) into v_actual_keys
  from pg_catalog.jsonb_object_keys(p_request) key;
  select pg_catalog.array_agg(key order by key) into v_expected_keys
  from pg_catalog.unnest(v_expected_keys) key;
  if v_actual_keys is distinct from v_expected_keys
     or p_request->>'schema_version'<>'WEEKLY_SOURCE_FINALISATION_PAY_TASK_START_V1'
     or coalesce(p_request->>'expected_context_hash','')!~'^[0-9a-f]{64}$' then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_TASK_START_CONTRACT_INVALID'
      using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_run_id:=(p_request->>'run_id')::uuid;
    v_task_id:=(p_request->>'task_id')::uuid;
    v_version:=(p_request->>'expected_task_version')::bigint;
    v_context_hash:=pg_catalog.decode(p_request->>'expected_context_hash','hex');
  exception when invalid_text_representation or numeric_value_out_of_range then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_TASK_START_INVALID' using errcode='22023';
  end;
  select * into v_run
  from public.weekly_source_finalisation_pay_runs run_row
  where run_row.id=v_run_id for update;
  select * into v_task
  from public.weekly_source_finalisation_pay_tasks task
  where task.id=v_task_id and task.run_id=v_run_id for update;
  if v_run.id is null or v_task.id is null
     or v_run.requested_by_user_id is distinct from v_actor
     or v_task.prepared_context_hash is distinct from v_context_hash then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_TASK_IDENTITY_CHANGED'
      using errcode='40001';
  end if;
  if v_task.state in (
    'PREPARED_FOR_AUTHORISATION','PROPOSED','NO_OP_FIRST_NEGATIVE','TARGET_MANAGED_SUPPRESSED','FAILED'
  ) then
    return pg_catalog.jsonb_build_object(
      'ok',v_task.state<>'FAILED',
      'idempotent_replay',true,'task',
      private.weekly_source_finalisation_pay_task_json_v1(v_task)
    );
  end if;
  if v_task.state in ('SUBMISSION_STARTED','RECOVERY_REQUIRED') then
    return pg_catalog.jsonb_build_object(
      'ok',false,'status','RECOVERY_REQUIRED',
      'error_code','WEEKLY_SOURCE_FINALISATION_PAY_RECOVERY_REQUIRED',
      'task',private.weekly_source_finalisation_pay_task_json_v1(v_task)
    );
  end if;
  if v_task.state<>'READY' or v_task.version<>v_version
     or v_run.state not in ('READY','RUNNING','ACTION_REQUIRED') then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_TASK_STALE'
      using errcode='40001';
  end if;
  update public.weekly_source_finalisation_pay_tasks task
  set state='SUBMISSION_STARTED',attempt_count=task.attempt_count+1,
      started_at_utc=coalesce(task.started_at_utc,pg_catalog.transaction_timestamp()),
      bounded_error_json=null,version=task.version+1
  where task.id=v_task.id returning * into v_task;
  update public.weekly_source_finalisation_pay_runs run_row
  set state='RUNNING',completed_at_utc=null where run_row.id=v_run.id;
  return pg_catalog.jsonb_build_object(
    'ok',true,'status','SUBMISSION_STARTED','idempotent_replay',false,
    'final_revision_id',v_run.final_revision_id,
    'projection_request',pg_catalog.jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_ORDINARY_PAY_PROJECTION_REQUEST_V1',
      'actor_user_id',v_actor,
      'final_revision_id',v_run.final_revision_id,
      'root_timesheet_id',v_task.root_timesheet_id,
      'idempotency_key',v_task.projection_idempotency_key
    ),
    'task',private.weekly_source_finalisation_pay_task_json_v1(v_task)
  );
end;
$function$;

create or replace function public.weekly_source_finalisation_pay_task_finish_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_expected_keys text[]:=array[
    'actor_user_id','expected_task_version','projection_receipt_hash',
    'projection_receipt_id','run_id','schema_version','task_id'
  ];
  v_actual_keys text[];
  v_actor uuid;
  v_run_id uuid;
  v_task_id uuid;
  v_receipt_id uuid;
  v_version bigint;
  v_receipt_hash bytea;
  v_run public.weekly_source_finalisation_pay_runs%rowtype;
  v_task public.weekly_source_finalisation_pay_tasks%rowtype;
  v_receipt public.weekly_source_ordinary_pay_projection_receipts%rowtype;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_TASK_FINISH_INVALID' using errcode='22023';
  end if;
  select pg_catalog.array_agg(key order by key) into v_actual_keys
  from pg_catalog.jsonb_object_keys(p_request) key;
  select pg_catalog.array_agg(key order by key) into v_expected_keys
  from pg_catalog.unnest(v_expected_keys) key;
  if v_actual_keys is distinct from v_expected_keys
     or p_request->>'schema_version'<>'WEEKLY_SOURCE_FINALISATION_PAY_TASK_FINISH_V1'
     or coalesce(p_request->>'projection_receipt_hash','')!~'^[0-9a-f]{64}$' then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_TASK_FINISH_CONTRACT_INVALID'
      using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_run_id:=(p_request->>'run_id')::uuid;
    v_task_id:=(p_request->>'task_id')::uuid;
    v_receipt_id:=(p_request->>'projection_receipt_id')::uuid;
    v_version:=(p_request->>'expected_task_version')::bigint;
    v_receipt_hash:=pg_catalog.decode(p_request->>'projection_receipt_hash','hex');
  exception when invalid_text_representation or numeric_value_out_of_range then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_TASK_FINISH_INVALID' using errcode='22023';
  end;
  select * into v_run
  from public.weekly_source_finalisation_pay_runs run_row
  where run_row.id=v_run_id for update;
  select * into v_task
  from public.weekly_source_finalisation_pay_tasks task
  where task.id=v_task_id and task.run_id=v_run_id for update;
  if v_run.id is null or v_task.id is null
     or v_run.requested_by_user_id is distinct from v_actor then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_TASK_IDENTITY_CHANGED'
      using errcode='40001';
  end if;
  if v_task.state in (
    'PREPARED_FOR_AUTHORISATION','PROPOSED','NO_OP_FIRST_NEGATIVE','TARGET_MANAGED_SUPPRESSED','FAILED'
  ) then
    v_run:=private.weekly_source_finalisation_pay_run_refresh_v1(v_run.id);
    return private.weekly_source_finalisation_pay_run_json_v1(v_run)
      ||pg_catalog.jsonb_build_object('idempotent_replay',true);
  end if;
  if v_task.state<>'SUBMISSION_STARTED' or v_task.version<>v_version then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_TASK_STALE'
      using errcode='40001';
  end if;
  select * into v_receipt
  from public.weekly_source_ordinary_pay_projection_receipts receipt
  where receipt.id=v_receipt_id
    and receipt.idempotency_key=v_task.projection_idempotency_key;
  if not found or v_receipt.final_revision_id is distinct from v_run.final_revision_id
     or v_receipt.root_timesheet_id is distinct from v_task.root_timesheet_id
     or v_receipt.receipt_hash is distinct from v_receipt_hash
     or v_receipt.outcome not in (
       'PREPARED_FOR_AUTHORISATION','PROPOSED','NO_OP_FIRST_NEGATIVE','TARGET_MANAGED_SUPPRESSED'
     ) then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_RECEIPT_INVALID'
      using errcode='55000';
  end if;
  perform pg_catalog.set_config(
    'cloudtms.weekly_source_projection_owner',
    'weekly_source_finalisation_pay_task_finish_atomic_v1',true
  );
  update public.weekly_source_state_transitions transition_row
  -- Gate 2: every reachable projection outcome is a success.  There is no
  -- REFUSED_LOCKED to map to FAILED any more.
  set ordinary_source_entitlement_projection_state='PUBLISHED'
  where transition_row.final_revision_id=v_run.final_revision_id
    and transition_row.ordinary_source_entitlement_projection_state='PENDING'
    and exists(
      select 1 from public.weekly_source_billing_movements movement
      where movement.transition_id=transition_row.id
        and movement.invoice_timesheet_id=v_task.root_timesheet_id
    );
  update public.weekly_source_finalisation_pay_tasks task
  set state=v_receipt.outcome,projection_receipt_id=v_receipt.id,
      bounded_error_json=null,
      completed_at_utc=pg_catalog.transaction_timestamp(),version=task.version+1
  where task.id=v_task.id;
  v_run:=private.weekly_source_finalisation_pay_run_refresh_v1(v_run.id);
  return private.weekly_source_finalisation_pay_run_json_v1(v_run)
    ||pg_catalog.jsonb_build_object('idempotent_replay',false);
end;
$function$;

create or replace function public.weekly_source_finalisation_pay_task_unknown_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_expected_keys text[]:=array[
    'actor_user_id','error_code','expected_task_version','run_id',
    'schema_version','task_id'
  ];
  v_actual_keys text[];
  v_actor uuid;
  v_run_id uuid;
  v_task_id uuid;
  v_version bigint;
  v_error_code text;
  v_run public.weekly_source_finalisation_pay_runs%rowtype;
  v_task public.weekly_source_finalisation_pay_tasks%rowtype;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_UNKNOWN_INVALID' using errcode='22023';
  end if;
  select pg_catalog.array_agg(key order by key) into v_actual_keys
  from pg_catalog.jsonb_object_keys(p_request) key;
  select pg_catalog.array_agg(key order by key) into v_expected_keys
  from pg_catalog.unnest(v_expected_keys) key;
  if v_actual_keys is distinct from v_expected_keys
     or p_request->>'schema_version'<>'WEEKLY_SOURCE_FINALISATION_PAY_UNKNOWN_V1' then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_UNKNOWN_CONTRACT_INVALID'
      using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_run_id:=(p_request->>'run_id')::uuid;
    v_task_id:=(p_request->>'task_id')::uuid;
    v_version:=(p_request->>'expected_task_version')::bigint;
  exception when invalid_text_representation or numeric_value_out_of_range then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_UNKNOWN_INVALID' using errcode='22023';
  end;
  v_error_code:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_request->>'error_code','')));
  if v_error_code!~'^[A-Z0-9_]{1,120}$' then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_UNKNOWN_INVALID' using errcode='22023';
  end if;
  select * into v_run from public.weekly_source_finalisation_pay_runs run_row
  where run_row.id=v_run_id for update;
  select * into v_task from public.weekly_source_finalisation_pay_tasks task
  where task.id=v_task_id and task.run_id=v_run_id for update;
  if v_run.id is null or v_task.id is null
     or v_run.requested_by_user_id is distinct from v_actor
     or v_task.version<>v_version then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_TASK_STALE'
      using errcode='40001';
  end if;
  if v_task.state='SUBMISSION_STARTED' then
    update public.weekly_source_finalisation_pay_tasks task
    set state='RECOVERY_REQUIRED',
        bounded_error_json=pg_catalog.jsonb_build_object(
          'error_code',v_error_code,'outcome','UNKNOWN'
        ),version=task.version+1
    where task.id=v_task.id returning * into v_task;
  end if;
  v_run:=private.weekly_source_finalisation_pay_run_refresh_v1(v_run.id);
  return private.weekly_source_finalisation_pay_run_json_v1(v_run)
    ||pg_catalog.jsonb_build_object('task',
      private.weekly_source_finalisation_pay_task_json_v1(v_task));
end;
$function$;

create or replace function public.weekly_source_finalisation_pay_task_recover_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_expected_keys text[]:=array[
    'actor_user_id','confirm_retry','expected_task_version','run_id',
    'schema_version','task_id'
  ];
  v_actual_keys text[];
  v_actor uuid;
  v_run_id uuid;
  v_task_id uuid;
  v_version bigint;
  v_confirm boolean;
  v_run public.weekly_source_finalisation_pay_runs%rowtype;
  v_task public.weekly_source_finalisation_pay_tasks%rowtype;
  v_receipt public.weekly_source_ordinary_pay_projection_receipts%rowtype;
  v_terminal_state text;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_RECOVER_INVALID' using errcode='22023';
  end if;
  select pg_catalog.array_agg(key order by key) into v_actual_keys
  from pg_catalog.jsonb_object_keys(p_request) key;
  select pg_catalog.array_agg(key order by key) into v_expected_keys
  from pg_catalog.unnest(v_expected_keys) key;
  if v_actual_keys is distinct from v_expected_keys
     or p_request->>'schema_version'<>'WEEKLY_SOURCE_FINALISATION_PAY_RECOVER_V1'
     or pg_catalog.jsonb_typeof(p_request->'confirm_retry')<>'boolean' then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_RECOVER_CONTRACT_INVALID'
      using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_run_id:=(p_request->>'run_id')::uuid;
    v_task_id:=(p_request->>'task_id')::uuid;
    v_version:=(p_request->>'expected_task_version')::bigint;
    v_confirm:=(p_request->>'confirm_retry')::boolean;
  exception when invalid_text_representation or numeric_value_out_of_range then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_RECOVER_INVALID' using errcode='22023';
  end;
  select * into v_run from public.weekly_source_finalisation_pay_runs run_row
  where run_row.id=v_run_id for update;
  select * into v_task from public.weekly_source_finalisation_pay_tasks task
  where task.id=v_task_id and task.run_id=v_run_id for update;
  if v_run.id is null or v_task.id is null
     or v_run.requested_by_user_id is distinct from v_actor
     or v_task.version<>v_version then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_TASK_STALE'
      using errcode='40001';
  end if;
  if v_task.state in (
    'PREPARED_FOR_AUTHORISATION','PROPOSED','NO_OP_FIRST_NEGATIVE','TARGET_MANAGED_SUPPRESSED','FAILED'
  ) then
    v_run:=private.weekly_source_finalisation_pay_run_refresh_v1(v_run.id);
    return private.weekly_source_finalisation_pay_run_json_v1(v_run)
      ||pg_catalog.jsonb_build_object('idempotent_replay',true);
  end if;
  if v_task.state not in ('SUBMISSION_STARTED','RECOVERY_REQUIRED') then
    raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_RECOVERY_NOT_REQUIRED'
      using errcode='55000';
  end if;
  select * into v_receipt
  from public.weekly_source_ordinary_pay_projection_receipts receipt
  where receipt.idempotency_key=v_task.projection_idempotency_key;
  if found then
    if v_receipt.final_revision_id is distinct from v_run.final_revision_id
       or v_receipt.root_timesheet_id is distinct from v_task.root_timesheet_id
       or v_receipt.outcome not in (
         'PREPARED_FOR_AUTHORISATION','PROPOSED','NO_OP_FIRST_NEGATIVE','TARGET_MANAGED_SUPPRESSED'
       ) then
      raise exception 'WEEKLY_SOURCE_FINALISATION_PAY_RECEIPT_INVALID'
        using errcode='55000';
    end if;
    v_terminal_state:=v_receipt.outcome;
    perform pg_catalog.set_config(
      'cloudtms.weekly_source_projection_owner',
      'weekly_source_finalisation_pay_task_recover_atomic_v1',true
    );
    update public.weekly_source_state_transitions transition_row
    set ordinary_source_entitlement_projection_state='PUBLISHED'
    where transition_row.final_revision_id=v_run.final_revision_id
      and transition_row.ordinary_source_entitlement_projection_state='PENDING'
      and exists(
        select 1 from public.weekly_source_billing_movements movement
        where movement.transition_id=transition_row.id
          and movement.invoice_timesheet_id=v_task.root_timesheet_id
      );
    update public.weekly_source_finalisation_pay_tasks task
    set state=v_terminal_state,projection_receipt_id=v_receipt.id,
        bounded_error_json=null,
        completed_at_utc=pg_catalog.transaction_timestamp(),version=task.version+1
    where task.id=v_task.id;
    v_run:=private.weekly_source_finalisation_pay_run_refresh_v1(v_run.id);
    return private.weekly_source_finalisation_pay_run_json_v1(v_run)
      ||pg_catalog.jsonb_build_object('recovered_receipt',true);
  end if;
  if not v_confirm then
    if v_task.state='SUBMISSION_STARTED' then
      update public.weekly_source_finalisation_pay_tasks task
      set state='RECOVERY_REQUIRED',bounded_error_json=coalesce(
        task.bounded_error_json,
        pg_catalog.jsonb_build_object(
          'error_code','WEEKLY_SOURCE_FINALISATION_PAY_RESULT_UNKNOWN',
          'outcome','UNKNOWN'
        )
      ),version=task.version+1
      where task.id=v_task.id;
    end if;
    v_run:=private.weekly_source_finalisation_pay_run_refresh_v1(v_run.id);
    return private.weekly_source_finalisation_pay_run_json_v1(v_run)
      ||pg_catalog.jsonb_build_object('retry_permitted',false);
  end if;
  update public.weekly_source_finalisation_pay_tasks task
  set state='READY',bounded_error_json=null,completed_at_utc=null,
      version=task.version+1
  where task.id=v_task.id returning * into v_task;
  update public.weekly_source_finalisation_pay_runs run_row
  set state='READY',completed_at_utc=null where run_row.id=v_run.id;
  v_run:=private.weekly_source_finalisation_pay_run_refresh_v1(v_run.id);
  return private.weekly_source_finalisation_pay_run_json_v1(v_run)
    ||pg_catalog.jsonb_build_object(
      'retry_permitted',true,
      'task',private.weekly_source_finalisation_pay_task_json_v1(v_task)
    );
end;
$function$;

alter function private.weekly_source_finalisation_pay_context_v1(uuid,uuid)
  owner to postgres;
alter function private.weekly_source_finalisation_pay_task_json_v1(
  public.weekly_source_finalisation_pay_tasks
) owner to postgres;
alter function private.weekly_source_finalisation_pay_run_refresh_v1(uuid)
  owner to postgres;
alter function private.weekly_source_finalisation_pay_run_json_v1(
  public.weekly_source_finalisation_pay_runs
) owner to postgres;
alter function public.weekly_source_finalisation_pay_open_atomic_v1(jsonb)
  owner to postgres;
alter function public.weekly_source_finalisation_pay_task_start_atomic_v1(jsonb)
  owner to postgres;
alter function public.weekly_source_finalisation_pay_task_finish_atomic_v1(jsonb)
  owner to postgres;
alter function public.weekly_source_finalisation_pay_task_unknown_atomic_v1(jsonb)
  owner to postgres;
alter function public.weekly_source_finalisation_pay_task_recover_atomic_v1(jsonb)
  owner to postgres;

revoke all on function private.weekly_source_finalisation_pay_context_v1(uuid,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_finalisation_pay_task_json_v1(
  public.weekly_source_finalisation_pay_tasks
) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_finalisation_pay_run_refresh_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_finalisation_pay_run_json_v1(
  public.weekly_source_finalisation_pay_runs
) from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_finalisation_pay_open_atomic_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_finalisation_pay_task_start_atomic_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_finalisation_pay_task_finish_atomic_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_finalisation_pay_task_unknown_atomic_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_finalisation_pay_task_recover_atomic_v1(jsonb)
  from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_finalisation_pay_open_atomic_v1(jsonb)
  to service_role;
grant execute on function public.weekly_source_finalisation_pay_task_start_atomic_v1(jsonb)
  to service_role;
grant execute on function public.weekly_source_finalisation_pay_task_finish_atomic_v1(jsonb)
  to service_role;
grant execute on function public.weekly_source_finalisation_pay_task_unknown_atomic_v1(jsonb)
  to service_role;
grant execute on function public.weekly_source_finalisation_pay_task_recover_atomic_v1(jsonb)
  to service_role;

comment on function public.weekly_source_finalisation_pay_open_atomic_v1(jsonb) is
  'Creates or replays the immutable per-root ordinary Timesheet/TSFIN projection manifest after final source has committed. It never gates source invoice authority.';
comment on function public.weekly_source_finalisation_pay_task_start_atomic_v1(jsonb) is
  'Durably records one ordinary-pay projection submission start before the existing projection owner is called.';
comment on function public.weekly_source_finalisation_pay_task_finish_atomic_v1(jsonb) is
  'Accepts only an independently persisted ordinary-pay projection receipt and checkpoints its exact terminal outcome.';
comment on function public.weekly_source_finalisation_pay_task_unknown_atomic_v1(jsonb) is
  'Records an unknown projection outcome as a durable recovery stop; it never retries.';
comment on function public.weekly_source_finalisation_pay_task_recover_atomic_v1(jsonb) is
  'Reconciles a durable receipt or, only with explicit confirmation, permits one idempotent retry after an unknown outcome.';

notify pgrst, 'reload schema';

commit;
