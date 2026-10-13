create or replace function private.timesheet_correction_request_guard_v1()
returns trigger language plpgsql security definer
set search_path to 'pg_catalog','private','public','pg_temp'
as $function$
begin
  if tg_op='DELETE' then raise exception 'CORRECTION_REQUEST_AUDIT_RETAINED' using errcode='55000'; end if;
  if tg_op='UPDATE' then
    if old.state<>'WAITING' then raise exception 'CORRECTION_REQUEST_TERMINAL_IMMUTABLE' using errcode='55000'; end if;
    if (to_jsonb(new)-array['state','state_revision','reversal_timesheet_id','reversal_booking_id','reversal_week_id',
        'workflow_id','workflow_generation','terminal_receipt_json','updated_at_utc','applied_at_utc','cancelled_at_utc'])
      is distinct from (to_jsonb(old)-array['state','state_revision','reversal_timesheet_id','reversal_booking_id','reversal_week_id',
        'workflow_id','workflow_generation','terminal_receipt_json','updated_at_utc','applied_at_utc','cancelled_at_utc']) then
      raise exception 'CORRECTION_REQUEST_ORIGINAL_PROPOSAL_IMMUTABLE' using errcode='55000';
    end if;
    if new.state_revision is distinct from old.state_revision+1 then
      raise exception 'CORRECTION_REQUEST_CAS_INVALID' using errcode='40001';
    end if;
  end if;
  return new;
end;
$function$;
drop trigger if exists timesheet_correction_request_guard_v1 on private.timesheet_correction_requests;
create trigger timesheet_correction_request_guard_v1 before update or delete
on private.timesheet_correction_requests for each row execute function private.timesheet_correction_request_guard_v1();

create or replace function private.timesheet_correction_whole_state_guard_v1()
returns trigger language plpgsql security definer
set search_path to 'pg_catalog','private','public','pg_temp'
as $function$
declare v_request private.timesheet_correction_requests%rowtype; v_r public.timesheets%rowtype; v_p public.timesheets%rowtype;
begin
  if new.correction_kind not in('WHOLE_TIMESHEET_REVERSAL','WHOLE_TIMESHEET_REPLACEMENT')
    or new.correction_kind is null then
    if tg_op='UPDATE' then
      if old.correction_kind in('WHOLE_TIMESHEET_REVERSAL','WHOLE_TIMESHEET_REPLACEMENT') then
        raise exception 'WHOLE_CORRECTION_MEMBER_IDENTITY_IMMUTABLE' using errcode='23514';
      end if;
    end if;
    return new;
  end if;
  select request.* into v_request from private.timesheet_correction_requests request
    where request.correction_id=new.correction_id;
  if v_request.id is null then raise exception 'WHOLE_CORRECTION_REQUEST_REQUIRED' using errcode='23514'; end if;
  if new.timesheet_id not in(v_request.replacement_timesheet_id,coalesce(v_request.reversal_timesheet_id,v_request.replacement_timesheet_id))
    or new.parent_timesheet_id is distinct from v_request.root_timesheet_id
    or new.adjustment_origin is distinct from v_request.origin
    or (new.timesheet_id=v_request.replacement_timesheet_id and
      (new.correction_kind<>'WHOLE_TIMESHEET_REPLACEMENT' or new.booking_id<>v_request.replacement_booking_id))
    or (new.timesheet_id=v_request.reversal_timesheet_id and
      (new.correction_kind<>'WHOLE_TIMESHEET_REVERSAL' or new.booking_id<>v_request.reversal_booking_id)) then
    raise exception 'WHOLE_CORRECTION_REQUEST_MEMBER_INVALID' using errcode='23514';
  end if;
  select t.* into v_p from public.timesheets t where t.timesheet_id=v_request.replacement_timesheet_id;
  select t.* into v_r from public.timesheets t where t.timesheet_id=v_request.reversal_timesheet_id;
  if v_request.state='WAITING' and (v_p.authorised_at_server is not null or v_r.authorised_at_server is not null) then
    raise exception 'WHOLE_CORRECTION_REQUIRES_ATOMIC_APPROVAL' using errcode='23514';
  end if;
  if v_request.state='APPLIED' and (v_r.timesheet_id is null or v_p.timesheet_id is null
    or (select count(*) from public.timesheets_financials tf where tf.timesheet_id in(v_r.timesheet_id,v_p.timesheet_id)
      and tf.is_current)<>2
    or (v_r.authorised_at_server is null) is distinct from (v_p.authorised_at_server is null)
    or exists(select 1 from public.timesheets_financials tf where tf.timesheet_id in(v_r.timesheet_id,v_p.timesheet_id)
      and tf.is_current and (tf.authorised_at_utc is null) is distinct from
        (case when tf.timesheet_id=v_r.timesheet_id then v_r.authorised_at_server is null else v_p.authorised_at_server is null end))) then
    raise exception 'WHOLE_CORRECTION_PAIR_STATE_INVALID' using errcode='23514';
  end if;
  return new;
end;
$function$;
drop trigger if exists zz_timesheet_correction_whole_state_guard_v1 on public.timesheets;
create constraint trigger zz_timesheet_correction_whole_state_guard_v1 after insert or update on public.timesheets
deferrable initially deferred for each row
execute function private.timesheet_correction_whole_state_guard_v1();
