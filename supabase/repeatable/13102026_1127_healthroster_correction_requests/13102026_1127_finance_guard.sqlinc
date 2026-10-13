create or replace function private.timesheet_correction_applied_assert_v1(p_request_id uuid)
returns void language plpgsql security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_request private.timesheet_correction_requests%rowtype; v_row record; v_role text;
begin
  select request.* into strict v_request from private.timesheet_correction_requests request where request.id=p_request_id;
  if v_request.state<>'APPLIED' then return; end if;
  if (select count(*) from public.timesheets t where t.timesheet_id in(v_request.reversal_timesheet_id,v_request.replacement_timesheet_id)
      and t.is_current and t.archived_at_utc is null and t.correction_id=v_request.correction_id
      and t.parent_timesheet_id=v_request.root_timesheet_id and t.adjustment_origin=v_request.origin)<>2
    or (select count(*) from public.timesheets t join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.timesheet_id in(v_request.reversal_timesheet_id,v_request.replacement_timesheet_id,v_request.reversed_timesheet_id)
        and t.is_current)<>3
    or (select count(*) from public.timesheets t join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
      where t.timesheet_id in(v_request.reversal_timesheet_id,v_request.replacement_timesheet_id))<>2
    or exists(select 1 from public.timesheets r join public.timesheets p on p.timesheet_id=v_request.replacement_timesheet_id
      where r.timesheet_id=v_request.reversal_timesheet_id
        and (r.authorised_at_server is null) is distinct from (p.authorised_at_server is null)) then
    raise exception 'WHOLE_CORRECTION_COMPLETE_UNIT_REQUIRED' using errcode='23514';
  end if;
  for v_row in select t.timesheet_id,t.authorised_at_server,to_jsonb(t) timesheet,to_jsonb(tf) financial,tf.id financial_id,tf.authorised_at_utc
    from public.timesheets t join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    where t.timesheet_id in(v_request.reversal_timesheet_id,v_request.replacement_timesheet_id,v_request.reversed_timesheet_id)
  loop
    v_role:=case when v_row.timesheet_id=v_request.reversal_timesheet_id then 'reversal'
      when v_row.timesheet_id=v_request.replacement_timesheet_id then 'replacement' else 'original' end;
    if v_row.financial_id::text is distinct from v_request.terminal_receipt_json->>(v_role||'_financial_id')
      or private.timesheet_correction_timesheet_authority_hash_v1(v_row.timesheet)
        is distinct from v_request.terminal_receipt_json->>(v_role||'_timesheet_authority_hash')
      or private.timesheet_correction_financial_authority_hash_v1(v_row.financial)
        is distinct from v_request.terminal_receipt_json->>(v_role||'_financial_authority_hash')
      or (v_role<>'original' and (v_row.authorised_at_server is null) is distinct from (v_row.authorised_at_utc is null))
      or (v_role='original' and coalesce((v_request.terminal_receipt_json->>'original_authorisation_required')::boolean,false)
        and (v_row.authorised_at_server is null or v_row.authorised_at_utc is null)) then
      raise exception 'WHOLE_CORRECTION_FROZEN_FINANCIAL_IDENTITY_CHANGED' using errcode='23514',detail=v_role;
    end if;
  end loop;
end;
$function$;

create or replace function private.timesheet_correction_financial_guard_v1()
returns trigger language plpgsql security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_id uuid; v_old_id uuid; v_request record;
begin
  v_id:=case when tg_op='DELETE' then old.timesheet_id else new.timesheet_id end;
  if tg_op in('UPDATE','DELETE') then v_old_id:=old.timesheet_id; end if;
  for v_request in select request.id from private.timesheet_correction_requests request
    where request.state='APPLIED' and (v_id in(request.reversed_timesheet_id,request.replacement_timesheet_id,request.reversal_timesheet_id)
      or v_old_id in(request.reversed_timesheet_id,request.replacement_timesheet_id,request.reversal_timesheet_id))
    order by request.id loop
    perform private.timesheet_correction_applied_assert_v1(v_request.id);
  end loop;
  return null;
end;
$function$;
drop trigger if exists zz_timesheet_correction_financial_guard_v1 on public.timesheets_financials;
create constraint trigger zz_timesheet_correction_financial_guard_v1 after insert or update or delete on public.timesheets_financials
  deferrable initially deferred for each row execute function private.timesheet_correction_financial_guard_v1();
drop trigger if exists zz_timesheet_correction_applied_timesheet_guard_v1 on public.timesheets;
create constraint trigger zz_timesheet_correction_applied_timesheet_guard_v1 after insert or update or delete on public.timesheets
  deferrable initially deferred for each row execute function private.timesheet_correction_financial_guard_v1();

create or replace function private.timesheet_correction_terminal_guard_v1()
returns trigger language plpgsql security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
begin
  perform private.timesheet_correction_applied_assert_v1(new.id);
  return null;
end;
$function$;
drop trigger if exists zz_timesheet_correction_terminal_guard_v1 on private.timesheet_correction_requests;
create constraint trigger zz_timesheet_correction_terminal_guard_v1 after insert or update on private.timesheet_correction_requests
  deferrable initially deferred for each row execute function private.timesheet_correction_terminal_guard_v1();
