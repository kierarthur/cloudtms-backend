-- One lifecycle predicate for inbox, badge, bulk actions, Undo and queued push.
-- Never use week age alone as proof that an outstanding action is resolved.
create or replace function private.candidate_notification_visible_v1(
  p_notification public.candidate_notifications, p_now_utc timestamptz,
  p_restored_state text default null
) returns boolean language plpgsql stable security definer set search_path = ''
as $function$
declare
  v_state text:=coalesce(p_restored_state,p_notification.state);
  v_generation uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_component public.candidate_expense_components%rowtype;
  v_component_generation integer;
begin
  if v_state not in ('READ','UNREAD') then return false; end if;
  -- Apply retention and explicit retirement even when evaluating a deleted
  -- notification's prior state for Undo. Lifecycle branches cannot bypass them.
  if p_notification.created_at_utc <= p_now_utc
    - (case when v_state='READ' then interval '30 days' else interval '90 days' end)
    or p_notification.deep_link_json->'obsolete'='true'::jsonb then return false; end if;
  if p_notification.deep_link_json->>'destination'='WEEKLY_SOURCE_REQUEST' then
    begin v_generation:=(p_notification.deep_link_json->>'request_id')::uuid;
    exception when invalid_text_representation then return false; end;
    return exists(
      select 1 from public.weekly_candidate_outreach_generations generation
      where generation.id=v_generation and generation.candidate_id=p_notification.candidate_id
        and generation.state='ACTIVE'
        and private.weekly_source_candidate_generation_current_v1(generation.id)
        and (
          (generation.request_kind='SUBMIT_TIMESHEET' and exists(
            select 1 from public.weekly_timesheet_submission_requests request
            join public.weekly_timesheet_submission_request_memberships membership on membership.submission_request_id=request.id
            where request.candidate_cohort_id=generation.candidate_cohort_id
              and request.source_cycle_id=generation.source_cycle_id
              and request.candidate_id=generation.candidate_id
              and request.request_generation=generation.generation_number
              and request.state in ('ACTIVE','OVERDUE','PARTLY_SUBMITTED') and membership.state='WAITING'
          )) or
          (generation.request_kind='CHECK_HOURS' and p_now_utc<=generation.deadline_at_utc and exists(
            select 1 from public.weekly_candidate_outreach_memberships membership
            join public.weekly_discrepancy_incidents incident on incident.id=membership.incident_id
            where membership.candidate_generation_id=generation.id and membership.state='ACTIONABLE'
              and incident.state='OPEN' and not private.weekly_source_covered_hours_incident_v1(incident.id)
          ))
        )
    );
  end if;
  -- Category rejection remains actionable even when its now-empty parent was
  -- finalised/cancelled. Exact component lineage takes precedence over parent state.
  if p_notification.event_type='OFFICE_REJECTED'
     and p_notification.template_params->>'resubmission_scope'='EXPENSE_CATEGORY' then
    begin
      select * into v_component from public.candidate_expense_components component
      where component.expense_component_id=(p_notification.template_params->>'expense_component_id')::uuid
        and component.workflow_id=p_notification.workflow_id;
    exception when invalid_text_representation then return false; end;
    if not found then return false; end if;
    select event.component_generation into v_component_generation
    from public.candidate_expense_component_events event
    where event.expense_component_id=v_component.expense_component_id
      and event.event_type='OFFICE_REJECTED' and event.occurred_at_utc=p_notification.created_at_utc
    order by event.component_generation desc limit 1;
    -- Missing historical metadata is not permission to discard an unresolved rejection.
    if v_component_generation is not null and v_component.component_generation<>v_component_generation then return false; end if;
    if v_component.lifecycle_state<>'OFFICE_REJECTED' then return false; end if;
    if exists(select 1 from public.candidate_expense_operations operation
      where operation.action_code='RESUBMIT_EXPENSE_CATEGORY' and operation.state='COMMITTED'
        and operation.candidate_id=p_notification.candidate_id
        and operation.result_json->>'source_expense_component_id'=v_component.expense_component_id::text
        and operation.result_json->>'source_component_generation'=v_component_generation::text
    ) then return false; end if;
    return true;
  end if;
  if p_notification.workflow_id is not null then
    select * into v_workflow from public.candidate_submission_workflows workflow
    where workflow.id=p_notification.workflow_id and workflow.account_id=p_notification.account_id
      and workflow.candidate_id=p_notification.candidate_id;
    if not found then return false; end if;
    if p_notification.event_type='PAPER_PACK_READY' then
      return v_workflow.route='PAPER' and v_workflow.state='AWAITING_PAPER_RETURN'
        and p_notification.template_params->>'workflow_generation'=v_workflow.generation::text
        and v_workflow.paper_return_manifest_sha256 is not null
        and p_notification.dedupe_key like '%:'||pg_catalog.encode(v_workflow.paper_return_manifest_sha256,'hex');
    end if;
    if p_notification.event_type in ('OFFICE_REJECTED','MANAGER_REFUSED') then
      return v_workflow.state in ('REJECTED','REFUSED')
        and not private._candidate_rejection_replaced_v1(v_workflow.id);
    end if;
    if v_workflow.state in ('CANCELLED','EXPIRED','SUPERSEDED')
      and p_notification.event_type not in ('CLAIM_CANCELLED','EXPENSE_CLAIM_CANCELLED','CLAIM_WITHDRAWN','EXPENSE_WITHDRAWN')
      then return false; end if;
  end if;
  return true;
end;
$function$;

create or replace function public.candidate_app_notifications_page_v1(
  p_session_id uuid, p_environment text, p_expected_rotation integer,
  p_limit integer default 14, p_cursor_created_at_utc timestamptz default null,
  p_cursor_id uuid default null, p_now_utc timestamptz default pg_catalog.now()
) returns jsonb language plpgsql stable security definer set search_path = ''
as $function$
declare v_context jsonb; v_rows jsonb; v_tail jsonb; v_more boolean;
begin
  perform private.weekly_source_query_require_service_v1();
  v_context:=private._candidate_session_context_v1(p_session_id,p_environment,p_expected_rotation,p_now_utc,false);
  if nullif(v_context->>'selected_candidate_id','') is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='42501'; end if;
  if p_limit is null or p_limit<1 or p_limit>100 then raise exception 'CANDIDATE_PAGE_LIMIT_INVALID' using errcode='22023'; end if;
  if (p_cursor_created_at_utc is null)<>(p_cursor_id is null) then raise exception 'CANDIDATE_NOTIFICATION_CURSOR_INVALID' using errcode='22023'; end if;
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(page) order by page.created_at_utc desc,page.id desc),'[]'::jsonb) into v_rows
  from (
    select notification.* from public.candidate_notifications notification
    where notification.account_id=(v_context->>'account_id')::uuid
      and notification.candidate_id=(v_context->>'selected_candidate_id')::uuid
      and private.candidate_notification_visible_v1(notification,p_now_utc)
      and (p_cursor_id is null or (notification.created_at_utc,notification.id)<(p_cursor_created_at_utc,p_cursor_id))
    order by notification.created_at_utc desc,notification.id desc limit p_limit+1
  ) page;
  v_more:=pg_catalog.jsonb_array_length(v_rows)>p_limit;
  if v_more then v_rows:=v_rows-p_limit; end if;
  v_tail:=v_rows->(pg_catalog.jsonb_array_length(v_rows)-1);
  return pg_catalog.jsonb_build_object('ok',true,'notifications',v_rows,'next_cursor',
    case when v_more then (v_tail->>'created_at_utc')||'|'||(v_tail->>'id') end);
end;
$function$;

create or replace function public.candidate_app_notifications_manage_v1(
  p_session_id uuid,p_environment text,p_expected_rotation integer,
  p_action text,p_request jsonb,p_now_utc timestamptz default pg_catalog.now()
) returns jsonb language plpgsql volatile security definer set search_path = ''
as $function$
declare
  v_context jsonb; v_account uuid; v_candidate uuid; v_key text;
  v_operation private.candidate_notification_operations%rowtype;
  v_snapshot private.candidate_notification_operations%rowtype;
  v_source private.candidate_notification_operations%rowtype;
  v_ids uuid[]:='{}'; v_id uuid:=pg_catalog.gen_random_uuid(); v_count integer:=0;
  v_skipped integer:=0; v_unread integer; v_result jsonb; v_item record;
begin
  perform private.weekly_source_query_require_service_v1();
  v_context:=private._candidate_session_context_v1(p_session_id,p_environment,p_expected_rotation,p_now_utc,true);
  v_account:=(v_context->>'account_id')::uuid; v_candidate:=(v_context->>'selected_candidate_id')::uuid;
  if v_candidate is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='42501'; end if;
  if p_action is null or p_action not in ('SNAPSHOT','MARK_READ','DELETE','UNDO') or p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
    then raise exception 'CANDIDATE_REQUEST_INVALID' using errcode='22023'; end if;
  v_key:=pg_catalog.btrim(p_request->>'idempotency_key');
  if v_key is null or pg_catalog.length(v_key) not between 1 and 200 then raise exception 'CANDIDATE_IDEMPOTENCY_KEY_REQUIRED' using errcode='22023'; end if;
  -- Serialize only this candidate inbox; independent candidate inboxes do not block.
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(v_account::text||':'||v_candidate::text,0));
  select * into v_operation from private.candidate_notification_operations operation
  where operation.account_id=v_account and operation.candidate_id=v_candidate and operation.idempotency_key=v_key;
  if found then
    if v_operation.action<>p_action or v_operation.request_json<>p_request then raise exception 'CANDIDATE_NOTIFICATION_IDEMPOTENCY_CONFLICT' using errcode='40001'; end if;
    return v_operation.result_json;
  end if;
  if p_action='SNAPSHOT' then
    if p_request->>'action' is null or p_request->>'action' not in ('MARK_READ','DELETE') or exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key where key not in ('action','idempotency_key'))
      then raise exception 'CANDIDATE_REQUEST_INVALID' using errcode='22023'; end if;
    select coalesce(pg_catalog.array_agg(notification.id order by notification.id),'{}'::uuid[]) into v_ids
    from public.candidate_notifications notification where notification.account_id=v_account and notification.candidate_id=v_candidate
      and private.candidate_notification_visible_v1(notification,p_now_utc)
      and (p_request->>'action'='DELETE' or notification.state='UNREAD');
    v_result:=pg_catalog.jsonb_build_object('ok',true,'snapshot_id',v_id,'affected_count',pg_catalog.cardinality(v_ids),'expires_at_utc',p_now_utc+interval '5 minutes');
  elsif p_action in ('MARK_READ','DELETE') then
    if exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key where key not in ('snapshot_id','notification_id','idempotency_key'))
      or ((p_request ? 'snapshot_id')=(p_request ? 'notification_id')) or (p_action='MARK_READ' and p_request ? 'notification_id')
      then raise exception 'CANDIDATE_REQUEST_INVALID' using errcode='22023'; end if;
    if p_request ? 'snapshot_id' then
      select * into v_snapshot from private.candidate_notification_operations operation
      where operation.id=(p_request->>'snapshot_id')::uuid and operation.account_id=v_account and operation.candidate_id=v_candidate
        and operation.action='SNAPSHOT' and operation.request_json->>'action'=case when p_action='DELETE' then 'DELETE' else 'MARK_READ' end;
      if not found then raise exception 'CANDIDATE_NOTIFICATION_NOT_FOUND' using errcode='P0002'; end if;
      if v_snapshot.expires_at_utc<=p_now_utc then raise exception 'CANDIDATE_NOTIFICATION_SNAPSHOT_EXPIRED' using errcode='22023'; end if;
      v_ids:=v_snapshot.notification_ids;
    else
      v_ids:=array[(p_request->>'notification_id')::uuid];
      if not exists(select 1 from public.candidate_notifications notification where notification.id=v_ids[1] and notification.account_id=v_account and notification.candidate_id=v_candidate)
        then raise exception 'CANDIDATE_NOTIFICATION_NOT_FOUND' using errcode='P0002'; end if;
    end if;
    -- Freeze the deletion receipt before storing its children; everything commits atomically.
    if p_action='DELETE' then
      insert into private.candidate_notification_operations(id,account_id,candidate_id,idempotency_key,action,request_json,notification_ids,result_json,created_at_utc,expires_at_utc)
      values(v_id,v_account,v_candidate,v_key,p_action,p_request,v_ids,'{}',p_now_utc,p_now_utc+interval '10 minutes');
    end if;
    for v_item in select notification.* from public.candidate_notifications notification
      where notification.id=any(v_ids) and notification.account_id=v_account and notification.candidate_id=v_candidate
      order by notification.id for update
    loop
      if not private.candidate_notification_visible_v1((select notification from public.candidate_notifications notification where notification.id=v_item.id),p_now_utc) then continue; end if;
      if p_action='MARK_READ' then
        if v_item.state<>'UNREAD' then continue; end if;
        update public.candidate_notifications set state='READ',read_at_utc=p_now_utc where id=v_item.id;
      else
        insert into private.candidate_notification_deleted_items values(v_id,v_item.id,v_item.state,v_item.read_at_utc);
        insert into private.candidate_notification_dismissal_owners values(v_item.id,v_id,p_now_utc)
        on conflict(notification_id) do update set operation_id=excluded.operation_id,dismissed_at_utc=excluded.dismissed_at_utc;
        update public.candidate_notifications set state='DISMISSED',dismissed_at_utc=p_now_utc where id=v_item.id;
      end if;
      v_count:=v_count+1;
    end loop;
  else
    if exists(select 1 from pg_catalog.jsonb_object_keys(p_request) key where key not in ('deletion_id','idempotency_key')) then raise exception 'CANDIDATE_REQUEST_INVALID' using errcode='22023'; end if;
    select * into v_source from private.candidate_notification_operations operation
    where operation.id=(p_request->>'deletion_id')::uuid and operation.account_id=v_account and operation.candidate_id=v_candidate and operation.action='DELETE';
    if not found then raise exception 'CANDIDATE_NOTIFICATION_NOT_FOUND' using errcode='P0002'; end if;
    if v_source.expires_at_utc<=p_now_utc then raise exception 'CANDIDATE_NOTIFICATION_UNDO_EXPIRED' using errcode='22023'; end if;
    for v_item in select notification.*,deleted.prior_state,deleted.prior_read_at_utc,
      dismissal.operation_id as current_deletion_id,dismissal.dismissed_at_utc as owner_dismissed_at
      from private.candidate_notification_deleted_items deleted
      join public.candidate_notifications notification on notification.id=deleted.notification_id
      left join private.candidate_notification_dismissal_owners dismissal on dismissal.notification_id=notification.id
      where deleted.operation_id=v_source.id and notification.account_id=v_account and notification.candidate_id=v_candidate
      order by notification.id for update of notification
    loop
      if v_item.state='DISMISSED' and v_item.current_deletion_id=v_source.id and v_item.dismissed_at_utc=v_item.owner_dismissed_at
        and private.candidate_notification_visible_v1((select notification from public.candidate_notifications notification where notification.id=v_item.id),p_now_utc,v_item.prior_state) then
        update public.candidate_notifications set state=v_item.prior_state,read_at_utc=v_item.prior_read_at_utc,dismissed_at_utc=null where id=v_item.id;
        v_count:=v_count+1;
      else v_skipped:=v_skipped+1; end if;
    end loop;
  end if;
  if p_action<>'SNAPSHOT' then
    select pg_catalog.count(*)::integer into v_unread from public.candidate_notifications notification
    where notification.account_id=v_account and notification.candidate_id=v_candidate and notification.state='UNREAD'
      and private.candidate_notification_visible_v1(notification,p_now_utc);
    v_result:=case p_action when 'MARK_READ' then pg_catalog.jsonb_build_object('ok',true,'affected_count',v_count,'unread_count',v_unread)
      when 'DELETE' then pg_catalog.jsonb_build_object('ok',true,'deletion_id',v_id,'affected_count',v_count,'undo_until_utc',p_now_utc+interval '10 minutes','unread_count',v_unread)
      else pg_catalog.jsonb_build_object('ok',true,'restored_count',v_count,'skipped_count',v_skipped,'unread_count',v_unread) end;
  end if;
  if p_action='DELETE' then update private.candidate_notification_operations set result_json=v_result where id=v_id;
  else insert into private.candidate_notification_operations(id,account_id,candidate_id,idempotency_key,action,request_json,notification_ids,result_json,created_at_utc,expires_at_utc)
    values(v_id,v_account,v_candidate,v_key,p_action,p_request,v_ids,v_result,p_now_utc,p_now_utc+case when p_action='SNAPSHOT' then interval '5 minutes' else interval '10 minutes' end);
  end if;
  return v_result;
end;
$function$;

alter function private.candidate_notification_visible_v1(public.candidate_notifications,timestamptz,text) owner to postgres;
alter function public.candidate_app_notifications_page_v1(uuid,text,integer,integer,timestamptz,uuid,timestamptz) owner to postgres;
alter function public.candidate_app_notifications_manage_v1(uuid,text,integer,text,jsonb,timestamptz) owner to postgres;
revoke all on function private.candidate_notification_visible_v1(public.candidate_notifications,timestamptz,text) from public,anon,authenticated,service_role;
revoke all on function public.candidate_app_notifications_page_v1(uuid,text,integer,integer,timestamptz,uuid,timestamptz) from public,anon,authenticated;
revoke all on function public.candidate_app_notifications_manage_v1(uuid,text,integer,text,jsonb,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_app_notifications_page_v1(uuid,text,integer,integer,timestamptz,uuid,timestamptz) to service_role;
grant execute on function public.candidate_app_notifications_manage_v1(uuid,text,integer,text,jsonb,timestamptz) to service_role;
