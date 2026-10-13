-- WB-37: exact native DUE metadata/receipt successor. Manual and at-cut owners retained.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_snooze_due_receipt_valid_v1(p_request_id uuid)
returns boolean language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare v_command private.bpay_next_command%rowtype;v_count integer;v_valid boolean;
begin
  select c.* into v_command from private.bpay_next_command c where c.id=p_request_id;
  if not found or v_command.command_kind<>'SNOOZE_CLEAR' or v_command.status<>'COMPLETE'
    or v_command.expected_member_count is distinct from 0 or v_command.enrolled_member_count<>0
    or v_command.sealed_at_utc is null
    or exists(select 1 from private.bpay_next_snooze_request r where r.request_id=p_request_id)
    or exists(select 1 from private.bpay_next_command_member m where m.command_id=p_request_id)
    or exists(select 1 from private.bpay_next_job j where j.command_id=p_request_id)
    or not exists(select 1 from private.bpay_next_module_control m where m.id=1
      and m.active_owner='NEXT' and m.owner_epoch=v_command.module_epoch) then return false;end if;
  -- request_id index and limit BEFORE joins/aggregation: never scan audit history.
  with page as materialized(select e.* from private.bpay_next_snooze_event e
    where e.request_id=p_request_id order by e.id limit 201)
  select count(*)::integer,coalesce(bool_and(coalesce(e.event_kind='WOKEN' and e.actor_id is null and e.reason='DUE'
      and e.control_sequence=v_command.agency_sequence and e.version>1
      and s.id is not null and s.version=e.version and s.last_request_id=p_request_id
      and e.after_state=to_jsonb(s) and not s.active and s.cleared_reason='DUE'
      and (e.before_state->>'active')::boolean and (e.before_state->>'cleared_reason') is null
      and (e.before_state->>'until_date')::date<(e.occurred_at_utc at time zone 'Europe/London')::date
      and (e.before_state->>'version')::bigint=e.version-1
      and (e.before_state->>'control_sequence')::bigint<e.control_sequence
      and e.candidate_id=s.candidate_id
      and prior.id is not null and e.before_state=jsonb_set(prior.after_state,'{cleared_reason}',
        coalesce(prior.after_state->'cleared_reason','null'::jsonb),true)
      and (e.after_state-array['active','version','control_sequence','updated_at_utc','last_request_id','cleared_reason'])
        =(e.before_state-array['active','version','control_sequence','updated_at_utc','last_request_id','cleared_reason'])
    ,false)),false) into v_count,v_valid from page e
    left join private.bpay_next_snooze s on s.id=e.snooze_id
    left join private.bpay_next_snooze_event prior on prior.snooze_id=e.snooze_id and prior.version=e.version-1;
  return v_count between 1 and 200 and v_valid;
end
$function$;
alter function private.bpay_next_snooze_due_receipt_valid_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_snooze_due_receipt_valid_v1(uuid) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_snooze_metadata_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_command private.bpay_next_command%rowtype;v_snooze private.bpay_next_snooze%rowtype;
  v_previous private.bpay_next_snooze_event%rowtype;v_context jsonb;v_args jsonb;v_request uuid;v_actor uuid;
  v_ack private.bpay_next_snooze_warning_ack%rowtype;v_due boolean:=false;
begin
  if tg_op in ('DELETE','TRUNCATE') then raise exception using errcode='55000',message='BPAY_NEXT_SNOOZE_METADATA_IMMUTABLE';end if;
  if tg_table_name='bpay_next_snooze' then v_request:=new.last_request_id;v_actor:=new.updated_by;
  elsif tg_table_name='bpay_next_snooze_warning_ack' then
    v_request:=case when tg_op='UPDATE' then new.consumed_by_request_id else new.request_id end;v_actor:=new.actor_id;
  else v_request:=new.request_id;v_actor:=new.actor_id;end if;
  select c.* into strict v_command from private.bpay_next_command c where c.id=v_request;
  v_due:=coalesce((tg_table_name='bpay_next_snooze' and tg_op='UPDATE' and not (to_jsonb(new)->>'active')::boolean
      and to_jsonb(new)->>'cleared_reason'='DUE')
    or (tg_table_name='bpay_next_snooze_event' and tg_op='INSERT' and to_jsonb(new)->>'event_kind'='WOKEN'),false);
  if v_due and (coalesce(current_setting('request.jwt.claim.role',true),
      nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
      or v_command.command_kind<>'SNOOZE_CLEAR') then
    raise exception using errcode='42501',message='BPAY_NEXT_SNOOZE_DUE_OWNER_INVALID';
  end if;
  if v_command.status<>'RECEIVED' or not exists(select 1 from private.bpay_next_module_control m
      where m.id=1 and m.active_owner='NEXT' and m.owner_epoch=v_command.module_epoch)
    or (not v_due and not exists(select 1 from public.tms_users u where u.id=v_actor and u.is_active and u.role::text='admin')) then
    raise exception using errcode='42501',message='BPAY_NEXT_SNOOZE_METADATA_OWNER_INVALID';
  end if;
  if tg_table_name='bpay_next_snooze' then
    if (v_due and (not old.active or old.until_date is null
        or old.until_date >= (clock_timestamp() at time zone 'Europe/London')::date
        or new.updated_by is distinct from old.updated_by or old.cleared_reason is not null))
      or (not v_due and new.cleared_reason is not null) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_DUE_SCOPE_INVALID';
    end if;
    if new.control_sequence<>v_command.agency_sequence or v_command.command_kind not in ('SNOOZE_SET','SNOOZE_CLEAR')
      or (tg_op='INSERT' and (new.version<>1 or not new.active or new.created_by<>new.updated_by or v_command.command_kind<>'SNOOZE_SET'))
      or (tg_op='UPDATE' and (not old.active or new.version<>old.version+1 or new.control_sequence<=old.control_sequence
        or (new.id,new.candidate_id,new.work_id,new.scope_kind,new.scope_identity,new.segment_key,new.case_id,new.case_component_id,
          new.created_at_utc,new.created_by) is distinct from (old.id,old.candidate_id,old.work_id,old.scope_kind,old.scope_identity,
          old.segment_key,old.case_id,old.case_component_id,old.created_at_utc,old.created_by))) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_METADATA_VERSION_INVALID';
    end if;
    if not new.active then
      if tg_op<>'UPDATE' or v_command.command_kind<>'SNOOZE_CLEAR'
        or (to_jsonb(new)-array['active','version','control_sequence','updated_at_utc','updated_by','last_request_id','cleared_reason'])
          is distinct from (to_jsonb(old)-array['active','version','control_sequence','updated_at_utc','updated_by','last_request_id','cleared_reason']) then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_CLEAR_FINANCIAL_TUPLE_CHANGED';
      end if;
      return new;
    end if;
    if v_command.command_kind<>'SNOOZE_SET' then raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_METADATA_KIND_INVALID';end if;
    v_args:=jsonb_strip_nulls(jsonb_build_object('candidate_id',new.candidate_id,'work_id',new.work_id,
      'expected_revision_id',new.expected_revision_id,'scope_kind',new.scope_kind,'segment_key',new.segment_key,
      'component_key',case when new.scope_kind='EXPENSE_LINE' then new.component_key end,'case_id',new.case_id,
      'case_component_id',new.case_component_id,'snooze_kind',new.snooze_kind,'until_date',new.until_date,
      'indefinite',new.until_date is null,'note',new.note));
    v_context:=private.bpay_next_snooze_context_v1(new.updated_by,v_args);
    if (new.scope_identity,new.expected_revision_id,new.physical_timesheet_id,new.approved_segment_id,
        new.expense_approved_line_id,new.component_key,new.case_kind,new.worker_pay_basis)
      is distinct from (v_context->>'scope_identity',(v_context->>'expected_revision_id')::uuid,
        (v_context->>'physical_timesheet_id')::uuid,(v_context->>'approved_segment_id')::uuid,
        (v_context->>'expense_approved_line_id')::uuid,v_context->>'component_key',v_context->>'case_kind',
        decode(v_context->>'worker_pay_basis','hex')) or v_context->>'conflict' is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_METADATA_SCOPE_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_snooze_event' then
    if tg_op<>'INSERT' then raise exception using errcode='55000',message='BPAY_NEXT_SNOOZE_METADATA_IMMUTABLE';end if;
    select s.* into strict v_snooze from private.bpay_next_snooze s where s.id=new.snooze_id;
    select e.* into v_previous from private.bpay_next_snooze_event e where e.snooze_id=new.snooze_id and e.version=new.version-1;
    if v_due then
      if new.actor_id is not null or new.reason<>'DUE' or v_snooze.active or v_snooze.cleared_reason<>'DUE'
        or v_previous.id is null or new.version<=1
        or new.before_state is distinct from jsonb_set(v_previous.after_state,'{cleared_reason}',
          coalesce(v_previous.after_state->'cleared_reason','null'::jsonb),true)
        or (new.candidate_id,new.version,new.control_sequence,new.request_id,new.after_state)
          is distinct from (v_snooze.candidate_id,v_snooze.version,v_snooze.control_sequence,
            v_snooze.last_request_id,to_jsonb(v_snooze)) or new.control_sequence<>v_command.agency_sequence then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_DUE_EVENT_INVALID';
      end if;
      return new;
    end if;
    if (new.candidate_id,new.version,new.control_sequence,new.actor_id,new.request_id,new.after_state)
      is distinct from (v_snooze.candidate_id,v_snooze.version,v_snooze.control_sequence,v_snooze.updated_by,
        v_snooze.last_request_id,to_jsonb(v_snooze)) or new.control_sequence<>v_command.agency_sequence
      or (new.version=1 and (new.event_kind<>'SET' or new.before_state is not null or not v_snooze.active))
      or (new.version>1 and (v_previous.id is null or new.before_state is distinct from jsonb_set(v_previous.after_state,'{cleared_reason}',coalesce(v_previous.after_state->'cleared_reason','null'::jsonb),true)
        or new.event_kind is distinct from (case when v_snooze.active then 'AMENDED'::text else 'CLEARED'::text end)))
      or v_command.command_kind is distinct from (case when new.event_kind='CLEARED' then 'SNOOZE_CLEAR'::text else 'SNOOZE_SET'::text end) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_EVENT_SCOPE_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_snooze_warning_ack' then
    if tg_op='INSERT' then
      if v_command.command_kind<>'SNOOZE_WARNING_ACK_ISSUE' or new.module_epoch<>v_command.module_epoch
        or new.consumed_by_request_id is not null or jsonb_typeof(new.warning_context)<>'object' then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_ISSUE_INVALID';
      end if;
    elsif tg_op<>'UPDATE' or old.consumed_by_request_id is not null or new.consumed_by_request_id is null
      or new.consumed_at_utc is null or new.consumed_at_utc>=old.expires_at_utc or new.consumed_at_utc<old.issued_at_utc
      or v_command.command_kind<>'SNOOZE_SET' or v_command.module_epoch<>old.module_epoch
      or (to_jsonb(new)-array['consumed_by_request_id','consumed_at_utc'])
        is distinct from (to_jsonb(old)-array['consumed_by_request_id','consumed_at_utc']) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_CONSUMPTION_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_snooze_request' and tg_op='INSERT' then
    if new.action<>v_command.command_kind or (new.request_body->>'request_id')::uuid is distinct from new.request_id
      or jsonb_typeof(new.request_body)<>'object' or octet_length(new.request_body::text)>4096
      or jsonb_typeof(new.result_body)<>'object' then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REQUEST_COMMAND_INVALID';
    end if;
    if new.action='SNOOZE_WARNING_ACK_ISSUE' then
      select a.* into strict v_ack from private.bpay_next_snooze_warning_ack a where a.request_id=new.request_id;
      v_context:=private.bpay_next_snooze_context_v1(new.actor_id,new.request_body);
      if (v_ack.actor_id,v_ack.candidate_id,v_ack.module_epoch,v_ack.scope_kind,v_ack.scope_identity,
          v_ack.expected_revision_id,v_ack.expected_version,v_ack.worker_pay_basis,v_ack.warning_context)
        is distinct from (new.actor_id,(v_context->>'candidate_id')::uuid,(v_context->>'module_epoch')::bigint,
          v_context->>'scope_kind',v_context->>'scope_identity',(v_context->>'expected_revision_id')::uuid,
          (v_context->>'expected_version')::bigint,decode(v_context->>'worker_pay_basis','hex'),v_context->'warning_context') then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_ISSUED_SCOPE_MISMATCH';
      end if;
    end if;
    return new;
  end if;
  raise exception using errcode='55000',message='BPAY_NEXT_SNOOZE_METADATA_IMMUTABLE';
end
$function$;
create or replace function private.bpay_next_snooze_receipt_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_request uuid;v_receipt private.bpay_next_snooze_request%rowtype;v_command private.bpay_next_command%rowtype;
  v_context jsonb;v_ack private.bpay_next_snooze_warning_ack%rowtype;
begin
  if tg_table_name='bpay_next_snooze' then v_request:=new.last_request_id;
  elsif tg_table_name='bpay_next_snooze_warning_ack' and tg_op='UPDATE' then v_request:=new.consumed_by_request_id;
  else v_request:=new.request_id;end if;
  if tg_table_name in ('bpay_next_snooze','bpay_next_snooze_event')
    and ((tg_table_name='bpay_next_snooze' and to_jsonb(new)->>'cleared_reason'='DUE')
      or (tg_table_name='bpay_next_snooze_event' and to_jsonb(new)->>'event_kind'='WOKEN')) then
    if not private.bpay_next_snooze_due_receipt_valid_v1(v_request) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_DUE_RECEIPT_INVALID';
    end if;
    return new;
  end if;
  select r.* into strict v_receipt from private.bpay_next_snooze_request r where r.request_id=v_request;
  select c.* into strict v_command from private.bpay_next_command c where c.id=v_request;
  if v_command.status<>'COMPLETE' or v_command.expected_member_count is distinct from 0 or v_command.sealed_at_utc is null
    or v_command.command_kind<>v_receipt.action then
    raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_COMMAND_RECEIPT_INCOMPLETE';
  end if;
  if tg_table_name in ('bpay_next_snooze','bpay_next_snooze_event') then
    if not exists(select 1 from private.bpay_next_snooze_event e
      where e.snooze_id=case when tg_table_name='bpay_next_snooze' then (to_jsonb(new)->>'id')::uuid else (to_jsonb(new)->>'snooze_id')::uuid end
        and e.version=new.version and e.request_id=v_request and e.actor_id=v_receipt.actor_id
        and e.candidate_id=(v_receipt.request_body->>'candidate_id')::uuid
        and (v_receipt.result_body->>'snooze_id')::uuid=e.snooze_id
        and (v_receipt.result_body->>'version')::bigint=e.version
        and (v_receipt.result_body->>'control_sequence')::bigint=e.control_sequence
        and (v_receipt.request_body->>'expected_version')::bigint=e.version-1
        and (v_receipt.action<>'SNOOZE_CLEAR' or (v_receipt.request_body->>'snooze_id')::uuid=e.snooze_id)) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_EVENT_RECEIPT_MISMATCH';
    end if;
  elsif tg_table_name='bpay_next_snooze_warning_ack' then
    if tg_op='INSERT' then
      if v_receipt.action<>'SNOOZE_WARNING_ACK_ISSUE' or v_receipt.actor_id<>new.actor_id
        or (v_receipt.request_body->>'candidate_id')::uuid is distinct from new.candidate_id
        or (v_receipt.result_body->>'acknowledgement_token')::uuid is distinct from new.token
        or (v_receipt.result_body->>'acknowledgement_expires_at_utc')::timestamptz is distinct from new.expires_at_utc
        or (v_receipt.result_body->>'scope_kind',v_receipt.result_body->>'scope_identity',
          (v_receipt.result_body->>'expected_revision_id')::uuid,(v_receipt.result_body->>'expected_version')::bigint,
          decode(v_receipt.result_body->>'worker_pay_basis','hex'),v_receipt.result_body->'warning_context')
          is distinct from (new.scope_kind,new.scope_identity,new.expected_revision_id,new.expected_version,new.worker_pay_basis,new.warning_context) then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_RECEIPT_MISMATCH';
      end if;
    else
      if v_receipt.action<>'SNOOZE_SET' or v_receipt.actor_id<>new.actor_id
        or (v_receipt.request_body->>'candidate_id')::uuid is distinct from new.candidate_id
        or (v_receipt.request_body->>'acknowledgement_token')::uuid is distinct from new.token
        or v_receipt.request_body->>'scope_kind' is distinct from new.scope_kind
        or (v_receipt.request_body->>'expected_revision_id')::uuid is distinct from new.expected_revision_id
        or (v_receipt.request_body->>'expected_version')::bigint is distinct from new.expected_version
        or decode(v_receipt.request_body->>'worker_pay_basis','hex') is distinct from new.worker_pay_basis
        or not exists(select 1 from private.bpay_next_snooze_event e where e.request_id=v_request
          and e.actor_id=new.actor_id and e.after_state->>'scope_identity'=new.scope_identity
          and (e.after_state->>'candidate_id')::uuid=new.candidate_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_CONSUMED_RECEIPT_MISMATCH';
      end if;
    end if;
  elsif tg_table_name='bpay_next_snooze_request' then
    if new.action='SNOOZE_WARNING_ACK_ISSUE' then
      if not exists(select 1 from private.bpay_next_snooze_warning_ack a where a.request_id=new.request_id
        and a.actor_id=new.actor_id and a.token=(new.result_body->>'acknowledgement_token')::uuid) then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_RECEIPT_MISSING';
      end if;
    elsif not exists(select 1 from private.bpay_next_snooze_event e where e.request_id=new.request_id
      and e.actor_id=new.actor_id and e.candidate_id=(new.request_body->>'candidate_id')::uuid
      and e.snooze_id=(new.result_body->>'snooze_id')::uuid and e.version=(new.result_body->>'version')::bigint
      and e.control_sequence=(new.result_body->>'control_sequence')::bigint
      and (e.after_state->>'active')::boolean is not distinct from (new.result_body->>'active')::boolean
      and (new.request_body->>'expected_version')::bigint=e.version-1
      and ((new.action='SNOOZE_CLEAR' and e.event_kind='CLEARED' and e.snooze_id=(new.request_body->>'snooze_id')::uuid)
        or (new.action='SNOOZE_SET' and e.event_kind in ('SET','AMENDED')
          and (e.after_state->>'work_id')::uuid is not distinct from (new.request_body->>'work_id')::uuid
          and (e.after_state->>'expected_revision_id')::uuid is not distinct from (new.request_body->>'expected_revision_id')::uuid
          and e.after_state->>'scope_kind'=new.request_body->>'scope_kind'
          and e.after_state->>'segment_key' is not distinct from new.request_body->>'segment_key'
          and (new.request_body->>'scope_kind'<>'EXPENSE_LINE' or e.after_state->>'component_key'=new.request_body->>'component_key')
          and (e.after_state->>'case_id')::uuid is not distinct from (new.request_body->>'case_id')::uuid
          and (e.after_state->>'case_component_id')::uuid is not distinct from (new.request_body->>'case_component_id')::uuid
          and e.after_state->>'snooze_kind'=case upper(btrim(new.request_body->>'snooze_kind'))
            when 'BLOCKED' then 'BLOCKED_TIMESHEET' when 'LOAN_REPAYMENT' then 'PAYMENT_ADVANCE_REPAYMENT'
            else upper(btrim(new.request_body->>'snooze_kind')) end
          and (e.after_state->>'worker_pay_basis')::bytea=decode(new.request_body->>'worker_pay_basis','hex')
          and (e.after_state->>'until_date')::date is not distinct from (new.request_body->>'until_date')::date
          and e.after_state->>'note'=coalesce(new.request_body->>'note','')))) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REQUEST_EVENT_MISMATCH';
    end if;
  end if;
  return new;
end
$function$;
alter function private.bpay_next_snooze_metadata_guard_v1() owner to postgres;
revoke all on function private.bpay_next_snooze_metadata_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_snooze_receipt_guard_v1() owner to postgres;
revoke all on function private.bpay_next_snooze_receipt_guard_v1() from public,anon,authenticated,service_role;
commit;
