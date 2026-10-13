-- WB-37: two bounded native recovery pages. No earnings/allocation/frozen changes.
\set ON_ERROR_STOP on
begin;
create or replace function public.bpay_next_snooze_due_page_v1(p_request jsonb)
returns jsonb language plpgsql security definer set search_path=pg_catalog,private
set statement_timeout='5s'
as $function$
declare
  v_limit integer;v_epoch bigint;v_today date:=(clock_timestamp() at time zone 'Europe/London')::date;
  v_seek_date date;v_seek_id uuid;v_last_date date;v_last_id uuid;
  v_page jsonb:='[]'::jsonb;v_row record;v_candidate uuid;v_id uuid;v_scope text;
  v_ready uuid[]:=array[]::uuid[];v_success text[]:=array[]::text[];
  v_visited integer:=0;v_changed integer:=0;v_complete boolean;
  v_request uuid;v_sequence bigint;v_before private.bpay_next_snooze%rowtype;v_after private.bpay_next_snooze%rowtype;
begin
  perform private.bpay_next_workbench_require_next_v1();
  select owner_epoch into strict v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if p_request is null or jsonb_typeof(p_request)<>'object' or octet_length(p_request::text)>1024
    or not(p_request ?& array['limit']) or p_request-array['limit']<>'{}'::jsonb
    or jsonb_typeof(p_request->'limit')<>'number' or (p_request->>'limit')!~'^[1-9][0-9]{0,2}$' then
    raise exception using errcode='22023',message='BPAY_NEXT_DUE_PAGE_INPUT_INVALID';
  end if;
  v_limit:=(p_request->>'limit')::integer;
  if v_limit>200 then raise exception using errcode='22023',message='BPAY_NEXT_DUE_PAGE_INPUT_INVALID';end if;
  -- Read before controls; never upgrade module ownership to a global write.
  select snooze_due_seek_date,snooze_due_seek_id into strict v_seek_date,v_seek_id
    from private.bpay_next_workbench_revision where id=1;
  for v_row in select s.id,s.until_date as due_date,s.candidate_id,s.work_id,s.case_id,s.version
      from private.bpay_next_snooze s where s.active and s.until_date<v_today
        and (v_seek_id is null or (s.until_date,s.id)>(v_seek_date,v_seek_id))
      order by s.until_date,s.id limit v_limit loop
    v_page:=jsonb_insert(v_page,'{-1}',jsonb_build_object('id',v_row.id,'date',v_row.due_date,
      'candidate_id',v_row.candidate_id,'work_id',v_row.work_id,'case_id',v_row.case_id,'version',v_row.version),true);
    v_last_date:=v_row.due_date;v_last_id:=v_row.id;v_visited:=v_visited+1;
  end loop;
  v_complete:=v_visited<v_limit;
  -- All Candidate preholds precede Work/Case/clock/projection. NOWAIT permits
  -- unrelated Candidates to advance, while the retained seek visits busy rows again.
  for v_candidate in select distinct (e->>'candidate_id')::uuid from jsonb_array_elements(v_page)e order by 1 loop
    begin
      perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update nowait;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CANDIDATE_CONTROL_MISSING';end if;
      v_ready:=array_append(v_ready,v_candidate);
    exception when lock_not_available or check_violation then null;end;
  end loop;
  for v_row in select distinct (e->>'work_id')::uuid as id,(e->>'candidate_id')::uuid as candidate_id
    from jsonb_array_elements(v_page)e where e->>'work_id' is not null order by 1 loop
    if not(v_row.candidate_id=any(v_ready)) then continue;end if;
    begin
      perform 1 from private.bpay_next_work w where w.id=v_row.id and w.candidate_id=v_row.candidate_id for update nowait;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_DUE_SCOPE_MISSING';end if;
    exception when lock_not_available or check_violation then v_ready:=array_remove(v_ready,v_row.candidate_id);end;
  end loop;
  for v_row in select distinct (e->>'case_id')::uuid id,(e->>'candidate_id')::uuid candidate_id
    from jsonb_array_elements(v_page)e where e->>'case_id' is not null order by 1 loop
    if not(v_row.candidate_id=any(v_ready)) then continue;end if;
    begin
      perform 1 from private.bpay_next_finance_case c where c.id=v_row.id and c.candidate_id=v_row.candidate_id for update nowait;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_DUE_SCOPE_MISSING';end if;
    exception when lock_not_available or check_violation then v_ready:=array_remove(v_ready,v_row.candidate_id);end;
  end loop;
  for v_row in select (e->>'id')::uuid id,(e->>'candidate_id')::uuid candidate_id
    from jsonb_array_elements(v_page)e order by 1 loop
    if not(v_row.candidate_id=any(v_ready)) then continue;end if;
    begin
      perform 1 from private.bpay_next_snooze s where s.id=v_row.id and s.candidate_id=v_row.candidate_id for update nowait;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_DUE_SCOPE_MISSING';end if;
    exception when lock_not_available or check_violation then v_ready:=array_remove(v_ready,v_row.candidate_id);end;
  end loop;
  -- One original CLEAR command per successful page, never per row. The block
  -- rolls its command/clock back if every exact scope fails; no orphan receipt.
  begin
    for v_row in select e from jsonb_array_elements(v_page)e order by (e->>'id')::uuid loop
      if not((v_row.e->>'candidate_id')::uuid=any(v_ready)) then continue;end if;
      select s.* into v_before from private.bpay_next_snooze s where s.id=(v_row.e->>'id')::uuid;
      if not found or not v_before.active or v_before.until_date is null or v_before.until_date>=v_today
        or v_before.version<>(v_row.e->>'version')::bigint
        or (v_before.candidate_id,v_before.work_id,v_before.case_id,v_before.until_date)
          is distinct from ((v_row.e->>'candidate_id')::uuid,(v_row.e->>'work_id')::uuid,
            (v_row.e->>'case_id')::uuid,(v_row.e->>'date')::date) then continue;end if;
      if v_request is null then
        v_request:=gen_random_uuid();v_sequence:=private.bpay_next_receive_command_v1(v_request,'SNOOZE_CLEAR');
      end if;
      begin
        update private.bpay_next_snooze s set active=false,cleared_reason='DUE',version=s.version+1,
          control_sequence=v_sequence,updated_at_utc=clock_timestamp(),last_request_id=v_request
          where s.id=v_before.id and s.version=v_before.version and s.active returning s.* into strict v_after;
        insert into private.bpay_next_snooze_event(snooze_id,candidate_id,event_kind,version,control_sequence,
          actor_id,request_id,reason,before_state,after_state)
          values(v_after.id,v_after.candidate_id,'WOKEN',v_after.version,v_sequence,null,v_request,'DUE',
            to_jsonb(v_before),to_jsonb(v_after));
        v_changed:=v_changed+1;
      exception when others then
        -- This scope's native row, event and exact projection trigger roll back
        -- together; nothing is cleared without an audit/refresh.
        null;
      end;
    end loop;
    if v_request is not null then
      if v_changed=0 then raise exception using errcode='PBP01',message='BPAY_NEXT_DUE_PAGE_NO_CHANGED_SCOPE';end if;
      update private.bpay_next_command set status='COMPLETE',expected_member_count=0,sealed_at_utc=clock_timestamp()
        where id=v_request and status='RECEIVED' and module_epoch=v_epoch;
      if not found or not private.bpay_next_snooze_due_receipt_valid_v1(v_request) then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_DUE_RECEIPT_INVALID';
      end if;
    end if;
  exception when sqlstate 'PBP01' then null;end;
  -- CAS protects overlapping pages. Cursor-only updates do not bump any
  -- context/financial revision. End wraps; a lost reply keeps durable progress.
  update private.bpay_next_workbench_revision set
    snooze_due_seek_date=case when v_complete then null else v_last_date end,
    snooze_due_seek_id=case when v_complete then null else v_last_id end
    where id=1 and (snooze_due_seek_date,snooze_due_seek_id) is not distinct from (v_seek_date,v_seek_id);
  return jsonb_build_object('rows_visited',v_visited::text,'rows_changed',v_changed::text,
    'rows_deferred',(v_visited-v_changed)::text,'complete',v_complete);
end
$function$;
alter function public.bpay_next_snooze_due_page_v1(jsonb) owner to postgres;
revoke all on function public.bpay_next_snooze_due_page_v1(jsonb) from public,anon,authenticated;
grant execute on function public.bpay_next_snooze_due_page_v1(jsonb) to service_role;

create or replace function public.bpay_next_workbench_week_rollover_page_v1(p_request jsonb)
returns jsonb language plpgsql security definer set search_path=pg_catalog,private
set statement_timeout='5s'
as $function$
declare
  v_limit integer;v_epoch bigint;v_today date:=(clock_timestamp() at time zone 'Europe/London')::date;
  v_seek_date date;v_seek_id uuid;v_last_date date;v_last_id uuid;
  v_page jsonb:='[]'::jsonb;v_row record;v_candidate uuid;v_id uuid;v_scope text;
  v_ready uuid[]:=array[]::uuid[];v_success text[]:=array[]::text[];
  v_visited integer:=0;v_changed integer:=0;v_complete boolean;
  v_request uuid;v_sequence bigint;v_before private.bpay_next_snooze%rowtype;v_after private.bpay_next_snooze%rowtype;
begin
  perform private.bpay_next_workbench_require_next_v1();
  select owner_epoch into strict v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if p_request is null or jsonb_typeof(p_request)<>'object' or octet_length(p_request::text)>1024
    or not(p_request ?& array['limit']) or p_request-array['limit']<>'{}'::jsonb
    or jsonb_typeof(p_request->'limit')<>'number' or (p_request->>'limit')!~'^[1-9][0-9]{0,2}$' then
    raise exception using errcode='22023',message='BPAY_NEXT_DUE_PAGE_INPUT_INVALID';
  end if;
  v_limit:=(p_request->>'limit')::integer;
  if v_limit>200 then raise exception using errcode='22023',message='BPAY_NEXT_DUE_PAGE_INPUT_INVALID';end if;
  -- Read before controls; never upgrade module ownership to a global write.
  select week_rollover_seek_date,week_rollover_seek_id into strict v_seek_date,v_seek_id
    from private.bpay_next_workbench_revision where id=1;
  for v_row in select i.id,i.eligible_from_date as due_date,i.candidate_id,i.work_id,i.case_id,0::bigint as version
      from private.bpay_next_workbench_item i where i.reason_code='FUTURE_WEEK' and i.eligible_from_date<=v_today
        and (v_seek_id is null or (i.eligible_from_date,i.id)>(v_seek_date,v_seek_id))
      order by i.eligible_from_date,i.id limit v_limit loop
    v_page:=jsonb_insert(v_page,'{-1}',jsonb_build_object('id',v_row.id,'date',v_row.due_date,
      'candidate_id',v_row.candidate_id,'work_id',v_row.work_id,'case_id',v_row.case_id,'version',v_row.version),true);
    v_last_date:=v_row.due_date;v_last_id:=v_row.id;v_visited:=v_visited+1;
  end loop;
  v_complete:=v_visited<v_limit;
  -- All Candidate preholds precede Work/Case/clock/projection. NOWAIT permits
  -- unrelated Candidates to advance, while the retained seek visits busy rows again.
  for v_candidate in select distinct (e->>'candidate_id')::uuid from jsonb_array_elements(v_page)e order by 1 loop
    begin
      perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update nowait;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CANDIDATE_CONTROL_MISSING';end if;
      v_ready:=array_append(v_ready,v_candidate);
    exception when lock_not_available or check_violation then null;end;
  end loop;
  for v_row in select distinct (e->>'work_id')::uuid as id,(e->>'candidate_id')::uuid as candidate_id
    from jsonb_array_elements(v_page)e where e->>'work_id' is not null order by 1 loop
    if not(v_row.candidate_id=any(v_ready)) then continue;end if;
    begin
      perform 1 from private.bpay_next_work w where w.id=v_row.id and w.candidate_id=v_row.candidate_id for update nowait;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_DUE_SCOPE_MISSING';end if;
    exception when lock_not_available or check_violation then v_ready:=array_remove(v_ready,v_row.candidate_id);end;
  end loop;
  for v_row in select distinct x.id,x.candidate_id from (
      select (e->>'case_id')::uuid id,(e->>'candidate_id')::uuid candidate_id
        from jsonb_array_elements(v_page)e where e->>'case_id' is not null
      union all select wc.case_id,w.candidate_id from (
        select distinct (e->>'work_id')::uuid id from jsonb_array_elements(v_page)e where e->>'work_id' is not null
      ) ids join private.bpay_next_work w on w.id=ids.id
      cross join lateral(select q.case_id from private.bpay_next_work_collection q
        where q.work_id=w.id and q.case_id is not null order by q.case_id limit 100) wc
    ) x order by x.id loop
    if not(v_row.candidate_id=any(v_ready)) then continue;end if;
    begin
      perform 1 from private.bpay_next_finance_case c where c.id=v_row.id and c.candidate_id=v_row.candidate_id for update nowait;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_DUE_SCOPE_MISSING';end if;
    exception when lock_not_available or check_violation then v_ready:=array_remove(v_ready,v_row.candidate_id);end;
  end loop;
  for v_row in select distinct case when e->>'work_id' is not null then 'WORK:'||(e->>'work_id')
      else 'CASE:'||(e->>'case_id') end as scope_key,(e->>'candidate_id')::uuid as candidate_id
    from jsonb_array_elements(v_page)e order by 1 loop
    if not(v_row.candidate_id=any(v_ready)) then continue;end if;
    v_scope:=v_row.scope_key;v_id:=substring(v_scope from 6)::uuid;
    if not exists(select 1 from private.bpay_next_workbench_item i
      where i.group_key=v_scope and i.candidate_id=v_row.candidate_id and i.reason_code='FUTURE_WEEK'
        and i.eligible_from_date<=v_today and i.id in(select (e->>'id')::uuid from jsonb_array_elements(v_page)e)) then continue;end if;
    begin
      if v_scope like 'WORK:%' then perform private.bpay_next_workbench_refresh_work_v1(v_id);
      else perform private.bpay_next_workbench_refresh_case_v1(v_id);end if;
      v_success:=array_append(v_success,v_scope);
    exception when others then null;end;
  end loop;
  select count(*)::integer into v_changed from jsonb_array_elements(v_page)e
    where (case when e->>'work_id' is not null then 'WORK:'||(e->>'work_id') else 'CASE:'||(e->>'case_id') end)=any(v_success);
  -- CAS protects overlapping pages. Cursor-only updates do not bump any
  -- context/financial revision. End wraps; a lost reply keeps durable progress.
  update private.bpay_next_workbench_revision set
    week_rollover_seek_date=case when v_complete then null else v_last_date end,
    week_rollover_seek_id=case when v_complete then null else v_last_id end
    where id=1 and (week_rollover_seek_date,week_rollover_seek_id) is not distinct from (v_seek_date,v_seek_id);
  return jsonb_build_object('rows_visited',v_visited::text,'rows_changed',v_changed::text,
    'rows_deferred',(v_visited-v_changed)::text,'complete',v_complete);
end
$function$;
alter function public.bpay_next_workbench_week_rollover_page_v1(jsonb) owner to postgres;
revoke all on function public.bpay_next_workbench_week_rollover_page_v1(jsonb) from public,anon,authenticated;
grant execute on function public.bpay_next_workbench_week_rollover_page_v1(jsonb) to service_role;

notify pgrst,'reload schema';
commit;
