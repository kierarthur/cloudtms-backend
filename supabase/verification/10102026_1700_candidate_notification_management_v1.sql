\set ON_ERROR_STOP on
-- Fixtures and assertions are transaction-owned and always rolled back.
begin;
select pg_catalog.set_config('request.jwt.claim.role','service_role',true);
do $proof$
declare
  v_now timestamptz:=pg_catalog.now(); v_a uuid:=gen_random_uuid(); v_b uuid:=gen_random_uuid();
  v_ca uuid:=gen_random_uuid(); v_cb uuid:=gen_random_uuid(); v_sa uuid:=gen_random_uuid(); v_sb uuid:=gen_random_uuid();
  v_notice uuid; v_read uuid; v_obsolete uuid; v_result jsonb; v_replay jsonb; v_snapshot jsonb; v_deleted jsonb;
  v_page jsonb; v_cursor text; v_seen uuid[]:='{}'; v_row jsonb; v_count integer:=0;
  v_email text:='notification-proof-'||gen_random_uuid()||'@example.test';
  v_prior_read timestamptz:=v_now-interval '1 hour';
  v_old_count integer; v_push text;
begin
  insert into public.candidates(id,email,display_name,active,key_norm)
    values(v_ca,v_email,'Notification A',true,'NOTIFICATION-A-'||v_ca),
          (v_cb,'other-'||v_email,'Notification B',true,'NOTIFICATION-B-'||v_cb);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
    values(v_a,'TEST',v_email,'ACTIVE'),(v_b,'TEST','other-'||v_email,'ACTIVE');
  insert into public.candidate_app_sessions(id,account_id,environment,selected_candidate_id,status,refresh_token_hash,expires_at_utc,absolute_expires_at_utc)
    values(v_sa,v_a,'TEST',v_ca,'ACTIVE',extensions.digest(v_sa::text,'sha256'),v_now+interval '1 day',v_now+interval '7 days'),
          (v_sb,v_b,'TEST',v_cb,'ACTIVE',extensions.digest(v_sb::text,'sha256'),v_now+interval '1 day',v_now+interval '7 days');
  insert into public.candidate_notifications(account_id,candidate_id,event_type,preference_category,template_key,dedupe_key,created_at_utc,push_state)
    select v_a,v_ca,'AUTHORISED','timesheet_updates','proof','notification-proof:'||gen_random_uuid(),v_now,'SENT' from generate_series(1,29);
  -- Candidate and account isolation, including another candidate under this account.
  insert into public.candidate_notifications(account_id,candidate_id,event_type,preference_category,template_key,dedupe_key,created_at_utc)
    values(v_b,v_cb,'AUTHORISED','timesheet_updates','proof','notification-proof:'||gen_random_uuid(),v_now),
          (v_a,v_cb,'AUTHORISED','timesheet_updates','proof','notification-proof:'||gen_random_uuid(),v_now);
  loop
    v_page:=public.candidate_app_notifications_page_v1(v_sa,'TEST',0,14,
      case when v_cursor is not null then split_part(v_cursor,'|',1)::timestamptz end,
      case when v_cursor is not null then split_part(v_cursor,'|',2)::uuid end,v_now);
    if jsonb_array_length(v_page->'notifications')<>(case when v_count<28 then 14 else 1 end) then raise exception 'PAGE_14_FILTER_OR_SCOPE_FAILED'; end if;
    for v_row in select value from jsonb_array_elements(v_page->'notifications') loop
      if (v_row->>'id')::uuid=any(v_seen) then raise exception 'PAGE_DUPLICATED_ID'; end if;
      v_seen:=array_append(v_seen,(v_row->>'id')::uuid);v_count:=v_count+1;
    end loop;
    v_cursor:=v_page->>'next_cursor'; exit when v_cursor is null;
  end loop;
  if v_count<>29 then raise exception 'PAGE_COUNT_FAILED'; end if;
  v_notice:=v_seen[1];v_read:=v_seen[2];v_obsolete:=v_seen[3];
  update public.candidate_notifications set state='READ',read_at_utc=v_prior_read where id=v_read;
  -- Retention uses creation time consistently across action and informational types.
  insert into public.candidate_notifications(account_id,candidate_id,event_type,preference_category,template_key,dedupe_key,created_at_utc,state)
    select v_a,v_ca,event_type,'timesheet_updates','proof','notification-proof:'||gen_random_uuid(),v_now-interval '91 days','UNREAD'
    from unnest(array['AUTHORISED','OFFICE_REJECTED','PAPER_PACK_READY','WEEKLY_SOURCE_REQUEST','CLAIM_CANCELLED']) event_type;
  insert into public.candidate_notifications(account_id,candidate_id,event_type,preference_category,template_key,dedupe_key,created_at_utc,state)
    values(v_a,v_ca,'AUTHORISED','timesheet_updates','proof','notification-proof:'||gen_random_uuid(),v_now-interval '31 days','READ');
  select count(*) into v_old_count from public.candidate_notifications n where n.account_id=v_a and n.created_at_utc<v_now-interval '30 days' and private.candidate_notification_visible_v1(n,v_now);
  if v_old_count<>0 then raise exception 'RETENTION_BYPASSED'; end if;
  v_result:=private._candidate_home_summary_v1('TEST',v_a,v_ca,'{}'::jsonb,v_now);
  if (v_result->'notifications'->>'unread_count')::integer<>28 then raise exception 'HOME_COUNT_SCOPE_OR_RETENTION_FAILED: %',v_result; end if;
  v_snapshot:=public.candidate_app_notifications_manage_v1(v_sa,'TEST',0,'SNAPSHOT',jsonb_build_object('action','DELETE','idempotency_key','snapshot-delete'),v_now);
  if (v_snapshot->>'affected_count')::integer<>29 then raise exception 'SNAPSHOT_INCLUDES_FOREIGN_OR_EXPIRED'; end if;
  insert into public.candidate_notifications(account_id,candidate_id,event_type,preference_category,template_key,dedupe_key,created_at_utc)
    values(v_a,v_ca,'AUTHORISED','timesheet_updates','proof','notification-proof:late-'||gen_random_uuid(),v_now);
  v_deleted:=public.candidate_app_notifications_manage_v1(v_sa,'TEST',0,'DELETE',jsonb_build_object('snapshot_id',v_snapshot->>'snapshot_id','idempotency_key','delete-all'),v_now);
  if (v_deleted->>'affected_count')::integer<>29 then raise exception 'DELETE_SNAPSHOT_CHANGED'; end if;
  v_replay:=public.candidate_app_notifications_manage_v1(v_sa,'TEST',0,'DELETE',jsonb_build_object('snapshot_id',v_snapshot->>'snapshot_id','idempotency_key','delete-all'),v_now+interval '1 second');
  if v_deleted<>v_replay then raise exception 'DELETE_REPLAY_DRIFT'; end if;
  begin
    perform public.candidate_app_notifications_manage_v1(v_sa,'TEST',0,'DELETE',jsonb_build_object('notification_id',v_notice,'idempotency_key','delete-all'),v_now);
    raise exception 'CHANGED_REPLAY_ACCEPTED';
  exception when sqlstate '40001' then null; end;
  begin
    perform public.candidate_app_notifications_manage_v1(v_sb,'TEST',0,'DELETE',jsonb_build_object('snapshot_id',v_snapshot->>'snapshot_id','idempotency_key','foreign-snapshot'),v_now);
    raise exception 'FOREIGN_SNAPSHOT_ACCEPTED';
  exception when sqlstate 'P0002' then null; end;
  begin
    perform public.candidate_app_notifications_manage_v1(v_sb,'TEST',0,'UNDO',jsonb_build_object('deletion_id',v_deleted->>'deletion_id','idempotency_key','foreign-undo'),v_now);
    raise exception 'FOREIGN_UNDO_ACCEPTED';
  exception when sqlstate 'P0002' then null; end;
  update public.candidate_notifications set deep_link_json='{"obsolete":true}' where id=v_obsolete;
  v_result:=public.candidate_app_notifications_manage_v1(v_sa,'TEST',0,'UNDO',jsonb_build_object('deletion_id',v_deleted->>'deletion_id','idempotency_key','undo-all'),v_now+interval '2 seconds');
  if (v_result->>'restored_count')::integer<>28 or (v_result->>'skipped_count')::integer<>1 then raise exception 'UNDO_OBSOLETE_RESURRECTED: %',v_result; end if;
  if not exists(select 1 from public.candidate_notifications where id=v_read and state='READ' and read_at_utc=v_prior_read) then raise exception 'UNDO_LOST_READ_STATE'; end if;
  select push_state into v_push from public.candidate_notifications where id=v_notice;
  if v_push<>'SENT' then raise exception 'UNDO_REQUEUED_PUSH'; end if;
  -- Original management receipt remains even when another authorised workflow
  -- physically removes a notification; no new FK may block that authority.
  delete from public.candidate_notifications where id=v_notice;
  if not exists(select 1 from private.candidate_notification_operations where id=(v_deleted->>'deletion_id')::uuid) then raise exception 'RECEIPT_LOST_ON_OFFICE_DELETE'; end if;
  if exists(select 1 from private.candidate_notification_deleted_items where notification_id=v_notice)
    or exists(select 1 from private.candidate_notification_dismissal_owners where notification_id=v_notice) then raise exception 'ORPHAN_MANAGEMENT_ITEM'; end if;
  v_snapshot:=public.candidate_app_notifications_manage_v1(v_sa,'TEST',0,'SNAPSHOT',jsonb_build_object('action','MARK_READ','idempotency_key','snapshot-read'),v_now);
  v_result:=public.candidate_app_notifications_manage_v1(v_sa,'TEST',0,'MARK_READ',jsonb_build_object('snapshot_id',v_snapshot->>'snapshot_id','idempotency_key','read-all'),v_now);
  if (v_result->>'affected_count')::integer<>27 or (v_result->>'unread_count')::integer<>0 then raise exception 'WHOLE_INBOX_READ_FAILED: %',v_result; end if;
  v_result:=private._candidate_home_summary_v1('TEST',v_a,v_ca,'{}'::jsonb,v_now);
  if (v_result->'notifications'->>'unread_count')::integer<>0 then raise exception 'HOME_BADGE_NOT_CLEARED'; end if;
  begin
    perform public.candidate_app_notifications_manage_v1(v_sa,'TEST',0,'UNDO',jsonb_build_object('deletion_id',v_deleted->>'deletion_id','idempotency_key','expired-undo'),v_now+interval '11 minutes');
    raise exception 'EXPIRED_UNDO_ACCEPTED';
  exception when sqlstate '22023' then null; end;
  raise notice 'NOTIFICATION MANAGEMENT PROOF PASS: paging, scope, expiry, exact snapshot, replay, Undo, original read state, provider neutrality and Office delete compatibility';
end;
$proof$;
do $lifecycle$
declare
  v_now timestamptz:=pg_catalog.now(); v_client uuid:=gen_random_uuid(); v_contract uuid:=gen_random_uuid(); v_week uuid:=gen_random_uuid();
  v_candidate uuid:=gen_random_uuid(); v_account uuid:=gen_random_uuid(); v_workflow uuid:=gen_random_uuid(); v_component uuid:=gen_random_uuid();
  v_notice public.candidate_notifications%rowtype; v_operation uuid:=gen_random_uuid(); v_hash bytea:=extensions.digest('notification-proof','sha256');
begin
  insert into public.clients(id,name) values(v_client,'Notification lifecycle proof');
  insert into public.client_settings(id,client_id,effective_from,default_submission_mode)
    values(gen_random_uuid(),v_client,'1900-01-01','ELECTRONIC');
  insert into public.candidates(id,email,display_name,active,key_norm) values(v_candidate,'lifecycle-'||v_candidate||'@example.test','Lifecycle proof',true,'LIFECYCLE-'||v_candidate);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status) values(v_account,'TEST','lifecycle-'||v_candidate||'@example.test','ACTIVE');
  insert into public.contracts(id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,week_ending_weekday_snapshot)
    values(v_contract,v_candidate,v_client,current_date-30,current_date+30,'PAYE',0);
  insert into public.contract_weeks(id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot)
    values(v_week,v_contract,current_date,0,'PLANNED','ELECTRONIC');
  insert into public.candidate_submission_workflows(id,environment,account_id,candidate_id,workflow_kind,scope,route,state,contract_id,contract_week_id,week_ending_date,idempotency_key)
    values(v_workflow,'TEST',v_account,v_candidate,'CONTRACT_EXPENSE','WEEKLY','ELECTRONIC','CANCELLED',v_contract,v_week,current_date,'lifecycle-proof');
  insert into public.candidate_expense_components(expense_component_id,workflow_id,workflow_generation,component_generation,expense_category,lifecycle_state,manager_approval_state,refusal_kind,refusal_reason,refused_at_utc,removed_at_utc)
    values(v_component,v_workflow,1,1,'ACCOMMODATION','OFFICE_REJECTED','NOT_REQUESTED','AGENCY_REJECTION','Proof rejection',v_now,v_now);
  insert into public.candidate_expense_component_events(expense_component_id,workflow_id,component_generation,event_type,actor_kind,idempotency_key,occurred_at_utc)
    values(v_component,v_workflow,1,'OFFICE_REJECTED','SYSTEM','lifecycle-proof-event',v_now);
  insert into public.candidate_notifications(account_id,candidate_id,workflow_id,event_type,preference_category,template_key,template_params,dedupe_key,created_at_utc)
    values(v_account,v_candidate,v_workflow,'OFFICE_REJECTED','timesheet_updates','proof',jsonb_build_object('resubmission_scope','EXPENSE_CATEGORY','expense_component_id',v_component),'lifecycle-proof:'||gen_random_uuid(),v_now)
    returning * into v_notice;
  if not private.candidate_notification_visible_v1(v_notice,v_now) then raise exception 'CATEGORY_HIDDEN_BY_CANCELLED_PARENT'; end if;
  insert into public.candidate_expense_operations(operation_id,environment,account_id,candidate_id,actor_kind,action_code,workflow_id,expense_component_id,request_sha256,idempotency_key,state,progress_json,created_at_utc,updated_at_utc)
    values(v_operation,'TEST',v_account,v_candidate,'CANDIDATE','RESUBMIT_EXPENSE_CATEGORY',v_workflow,v_component,v_hash,'lifecycle-resubmit','PREPARING',jsonb_build_object('source_expense_component_id',v_component,'source_component_generation',1),v_now,v_now);
  if not private.candidate_notification_visible_v1(v_notice,v_now) then raise exception 'PREPARING_RETIRED_CATEGORY'; end if;
  update public.candidate_expense_operations set state='RENDERING' where operation_id=v_operation;
  if not private.candidate_notification_visible_v1(v_notice,v_now) then raise exception 'RENDERING_RETIRED_CATEGORY'; end if;
  update public.candidate_expense_operations set state='COMMITTED',completed_at_utc=v_now,result_json=jsonb_build_object('source_expense_component_id',gen_random_uuid(),'source_component_generation',1) where operation_id=v_operation;
  if not private.candidate_notification_visible_v1(v_notice,v_now) then raise exception 'UNRELATED_COMPONENT_RETIRED_CATEGORY'; end if;
  update public.candidate_expense_operations set result_json=jsonb_build_object('source_expense_component_id',v_component,'source_component_generation',1) where operation_id=v_operation;
  if private.candidate_notification_visible_v1(v_notice,v_now) then raise exception 'EXACT_COMMIT_DID_NOT_RETIRE_CATEGORY'; end if;
  update public.candidate_expense_operations set result_json=jsonb_build_object('source_expense_component_id',gen_random_uuid(),'source_component_generation',1) where operation_id=v_operation;
  update public.candidate_expense_components set component_generation=2 where expense_component_id=v_component;
  if private.candidate_notification_visible_v1(v_notice,v_now) then raise exception 'REPLACED_COMPONENT_REJECTION_VISIBLE'; end if;
  v_notice.event_type:='CLAIM_CANCELLED';v_notice.template_params:='{}';v_notice.created_at_utc:=v_now;
  if not private.candidate_notification_visible_v1(v_notice,v_now) then raise exception 'CANCELLATION_CONFIRMATION_HIDDEN'; end if;
  if private.candidate_notification_visible_v1(v_notice,v_now+interval '91 days') then raise exception 'CANCELLATION_CONFIRMATION_NEVER_EXPIRES'; end if;
  update public.candidate_submission_workflows set state='AWAITING_PAPER_RETURN',route='PAPER',paper_return_manifest_json='{"pages":[{}]}',paper_return_manifest_sha256=v_hash where id=v_workflow;
  v_notice.event_type:='PAPER_PACK_READY';v_notice.template_params:='{"workflow_generation":1}';v_notice.dedupe_key:='paper:'||encode(v_hash,'hex');
  if not private.candidate_notification_visible_v1(v_notice,v_now) then raise exception 'CURRENT_PAPER_HIDDEN'; end if;
  v_notice.template_params:='{"workflow_generation":2}';
  if private.candidate_notification_visible_v1(v_notice,v_now) then raise exception 'OLD_PAPER_GENERATION_VISIBLE'; end if;
  v_notice.template_params:='{"workflow_generation":1}';v_notice.deep_link_json:='{"obsolete":true}';v_notice.state:='DISMISSED';
  if private.candidate_notification_visible_v1(v_notice,v_now,'UNREAD') then raise exception 'OBSOLETE_PAPER_UNDO_ALLOWED'; end if;
  raise notice 'NOTIFICATION LIFECYCLE PROOF PASS: category exact committed lineage, cancelled-parent exception, informational confirmations and exact paper generation';
end;
$lifecycle$;
rollback;
