-- WB-29 native chosen date repair in the original WEEK_ROLLOVER lane; BD168.
\set ON_ERROR_STOP on
begin;

create or replace function public.bpay_next_workbench_week_rollover_page_v1(p_request jsonb)
returns jsonb language plpgsql security definer set search_path=pg_catalog,private
set statement_timeout='5s'
as $function$
declare
  v_limit integer;v_epoch bigint;v_today date:=(clock_timestamp() at time zone 'Europe/London')::date;
  v_seek_date date;v_seek_id uuid;v_last_date date;v_last_id uuid;
  v_page jsonb:='[]'::jsonb;v_row record;v_candidate uuid;v_id uuid;v_scope text;
  v_ready uuid[]:=array[]::uuid[];v_success text[]:=array[]::text[];
  v_chosen jsonb:='[]'::jsonb;v_all_page jsonb:='[]'::jsonb;v_record jsonb;
  v_chosen_success uuid[]:=array[]::uuid[];
  v_date_context jsonb;v_pay_date date;v_d_seek_date date;v_d_seek_id uuid;v_d_last_date date;v_d_last_id uuid;
  v_f_cursor_date date;v_f_cursor_id uuid;v_phase integer;v_quota integer;v_found integer;
  v_f_probed boolean:=false;v_d_probed boolean:=false;v_f_done boolean:=false;v_d_done boolean:=false;
  v_future_first boolean:=mod(floor(extract(epoch from clock_timestamp())/60)::bigint,2)=0;
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
  select week_rollover_seek_date,week_rollover_seek_id,chosen_rollover_seek_date,chosen_rollover_seek_work_id
    into strict v_seek_date,v_seek_id,v_d_seek_date,v_d_seek_id from private.bpay_next_workbench_revision where id=1;
  v_f_cursor_date:=v_seek_date;v_f_cursor_id:=v_seek_id;
  v_date_context:=public.pay_banking_official_date_context_v1(null::timestamptz);
  if v_date_context->>'ok' is distinct from 'true' or v_date_context->>'current_official_pay_date' is null then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_DATE_UNAVAILABLE';end if;
  v_pay_date:=(v_date_context->>'current_official_pay_date')::date;
  -- Discover both finite prefixes before any business mutation. Unused quota
  -- is donated; complete retains the original visited<limit transport meaning.
  for v_phase in 1..3 loop
    if v_phase=1 then
      v_quota:=case when v_limit=1 then case when v_future_first then 1 else 0 end else (v_limit+1)/2 end;
    elsif v_phase=2 then v_quota:=v_limit-v_visited;
    else
      if not v_d_done or v_f_done then continue;end if;
      v_quota:=v_limit-v_visited;
    end if;
    if v_quota=0 then continue;end if;
    v_found:=0;
    if v_phase in (1,3) then
      v_f_probed:=true;
      for v_row in select i.id,i.eligible_from_date as due_date,i.candidate_id,i.work_id,i.case_id,0::bigint as version
        from private.bpay_next_workbench_item i where i.reason_code='FUTURE_WEEK' and i.eligible_from_date<=v_today
          and (v_f_cursor_id is null or (i.eligible_from_date,i.id)>(v_f_cursor_date,v_f_cursor_id))
        order by i.eligible_from_date,i.id limit v_quota loop
        v_record:=jsonb_build_object('id',v_row.id,'date',v_row.due_date,'candidate_id',v_row.candidate_id,
          'work_id',v_row.work_id,'case_id',v_row.case_id,'version',v_row.version);
        v_page:=jsonb_insert(v_page,'{-1}',v_record,true);
        v_all_page:=jsonb_insert(v_all_page,'{-1}',v_record,true);
        v_last_date:=v_row.due_date;v_last_id:=v_row.id;v_found:=v_found+1;v_visited:=v_visited+1;
      end loop;
      v_f_done:=v_found<v_quota;
      if v_found>0 then v_f_cursor_date:=v_last_date;v_f_cursor_id:=v_last_id;end if;
    else
      v_d_probed:=true;
      -- Three separately indexed ranges, each bounded before their finite merge.
      -- The native NULL-first seek permits a visited NULL-date/non-NULL-ID tuple.
      for v_row in
        with missing as materialized(select g.work_id,g.candidate_id,g.chosen_display_pay_date due_date
          from private.bpay_next_workbench_group g where g.group_kind='WORK' and g.chosen_display_date_sensitive and g.complete and not g.dirty
            and g.progress_state='CURRENT' and (g.ready_count>0 or g.action_count>0)
            and g.chosen_display_pay_date is null
            and (v_d_seek_id is null or v_d_seek_date is null and g.work_id>v_d_seek_id)
          order by g.chosen_display_pay_date nulls first,g.work_id limit v_quota),
        older as materialized(select g.work_id,g.candidate_id,g.chosen_display_pay_date due_date
          from private.bpay_next_workbench_group g where g.group_kind='WORK' and g.chosen_display_date_sensitive and g.complete and not g.dirty
            and g.progress_state='CURRENT' and (g.ready_count>0 or g.action_count>0)
            and g.chosen_display_pay_date<v_pay_date
            and (v_d_seek_id is null or v_d_seek_date is null or
              (g.chosen_display_pay_date,g.work_id)>(v_d_seek_date,v_d_seek_id))
          order by g.chosen_display_pay_date nulls first,g.work_id limit v_quota),
        newer as materialized(select g.work_id,g.candidate_id,g.chosen_display_pay_date due_date
          from private.bpay_next_workbench_group g where g.group_kind='WORK' and g.chosen_display_date_sensitive and g.complete and not g.dirty
            and g.progress_state='CURRENT' and (g.ready_count>0 or g.action_count>0)
            and g.chosen_display_pay_date>v_pay_date
            and (v_d_seek_id is null or v_d_seek_date is null or
              (g.chosen_display_pay_date,g.work_id)>(v_d_seek_date,v_d_seek_id))
          order by g.chosen_display_pay_date nulls first,g.work_id limit v_quota)
        select x.* from (select * from missing union all select * from older union all select * from newer) x
        order by x.due_date nulls first,x.work_id limit v_quota loop
        v_record:=jsonb_build_object('id',v_row.work_id,'date',v_row.due_date,'candidate_id',v_row.candidate_id,
          'work_id',v_row.work_id,'case_id',null);
        v_chosen:=jsonb_insert(v_chosen,'{-1}',v_record,true);
        v_all_page:=jsonb_insert(v_all_page,'{-1}',v_record,true);
        v_d_last_date:=v_row.due_date;v_d_last_id:=v_row.work_id;v_found:=v_found+1;v_visited:=v_visited+1;
      end loop;
      v_d_done:=v_found<v_quota;
    end if;
  end loop;
  v_complete:=v_visited<v_limit;
  -- All Candidate preholds precede Work/Case/clock/projection. NOWAIT permits
  -- unrelated Candidates to advance, while the retained seek visits busy rows again.
  for v_candidate in select distinct (e->>'candidate_id')::uuid from jsonb_array_elements(v_all_page)e order by 1 loop
    begin
      perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update nowait;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CANDIDATE_CONTROL_MISSING';end if;
      v_ready:=array_append(v_ready,v_candidate);
    exception when lock_not_available or check_violation then null;end;
  end loop;
  for v_row in select distinct (e->>'work_id')::uuid as id,(e->>'candidate_id')::uuid as candidate_id
    from jsonb_array_elements(v_all_page)e where e->>'work_id' is not null order by 1 loop
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
      cross join lateral(select distinct q.case_id from private.bpay_next_work_collection q
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
  for v_row in select (e->>'work_id')::uuid work_id,(e->>'candidate_id')::uuid candidate_id
    from jsonb_array_elements(v_chosen)e order by 1 loop
    if not(v_row.candidate_id=any(v_ready)) then continue;end if;
    begin
      if exists(select 1 from private.bpay_next_workbench_group g where g.work_id=v_row.work_id
        and g.candidate_id=v_row.candidate_id and g.group_kind='WORK' and g.chosen_display_date_sensitive and g.complete and not g.dirty
            and g.progress_state='CURRENT' and (g.ready_count>0 or g.action_count>0)
        and (g.chosen_display_pay_date is null or g.chosen_display_pay_date<>v_pay_date)) then
        perform private.bpay_next_workbench_queue_origin_v1(v_row.work_id);
        v_chosen_success:=array_append(v_chosen_success,v_row.work_id);
      elsif ('WORK:'||v_row.work_id::text)=any(v_success) and exists(
        select 1 from private.bpay_next_workbench_group g where g.work_id=v_row.work_id and g.candidate_id=v_row.candidate_id
          and g.complete and not g.dirty and g.progress_state='CURRENT'
          and (not g.chosen_display_date_sensitive or g.chosen_display_pay_date=v_pay_date)) then
        -- Same-call FUTURE refresh positively completed this exact date scope.
        v_chosen_success:=array_append(v_chosen_success,v_row.work_id);
      end if;
    exception when others then null;end;
  end loop;
  v_changed:=v_changed+cardinality(v_chosen_success);

  -- CAS protects overlapping pages. Cursor-only updates do not bump any
  -- context/financial revision. End wraps; a lost reply keeps durable progress.
  if v_f_probed then
    update private.bpay_next_workbench_revision set
      week_rollover_seek_date=case when v_f_done then null else v_last_date end,
      week_rollover_seek_id=case when v_f_done then null else v_last_id end
      where id=1 and (week_rollover_seek_date,week_rollover_seek_id) is not distinct from (v_seek_date,v_seek_id);
  end if;
  if v_d_probed then
    update private.bpay_next_workbench_revision set
      chosen_rollover_seek_date=case when v_d_done then null else v_d_last_date end,
      chosen_rollover_seek_work_id=case when v_d_done then null else v_d_last_id end
      where id=1 and (chosen_rollover_seek_date,chosen_rollover_seek_work_id) is not distinct from (v_d_seek_date,v_d_seek_id);
  end if;
  return jsonb_build_object('rows_visited',v_visited::text,'rows_changed',v_changed::text,
    'rows_deferred',(v_visited-v_changed)::text,'complete',v_complete);
end
$function$;

alter function public.bpay_next_workbench_week_rollover_page_v1(jsonb) owner to postgres;
revoke all on function public.bpay_next_workbench_week_rollover_page_v1(jsonb) from public,anon,authenticated;
grant execute on function public.bpay_next_workbench_week_rollover_page_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
