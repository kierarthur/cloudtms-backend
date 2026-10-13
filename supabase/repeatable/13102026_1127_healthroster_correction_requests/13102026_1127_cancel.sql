-- Owner-created, transaction-bound capability. It is not a client flag or a
-- broad workflow retirement exemption and contains no durable business store.
create or replace function private.timesheet_correction_cancel_capability_v1(p_timesheet_id uuid,p_actor_user_id uuid default null)
returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare v_cap jsonb; v_request private.timesheet_correction_requests%rowtype; v_p public.timesheets%rowtype;
begin
  if to_regclass('pg_temp.timesheet_correction_cancel_capability_v1') is null
    or not exists(select 1 from pg_class c where c.oid=to_regclass('pg_temp.timesheet_correction_cancel_capability_v1')
      and c.relowner=(select p.proowner from pg_proc p where p.oid='private.timesheet_correction_cancel_capability_v1(uuid,uuid)'::regprocedure)) then
    return null;
  end if;
  execute 'select to_jsonb(cap) from pg_temp.timesheet_correction_cancel_capability_v1 cap where cap.timesheet_id=$1 and cap.backend_pid=pg_backend_pid() and cap.transaction_id=txid_current()'
    into v_cap using p_timesheet_id;
  if v_cap is null or (p_actor_user_id is not null and v_cap->>'actor_user_id' is distinct from p_actor_user_id::text) then return null; end if;
  select request.* into v_request from private.timesheet_correction_requests request where request.id=(v_cap->>'request_id')::uuid;
  select t.* into v_p from public.timesheets t where t.timesheet_id=p_timesheet_id;
  if v_request.state is distinct from 'WAITING' or v_request.state_revision::text is distinct from v_cap->>'state_revision'
    or v_request.replacement_timesheet_id is distinct from p_timesheet_id
    or v_p.timesheet_id is null or not v_p.is_current or v_p.authorised_at_server is not null
    or v_p.booking_id is distinct from v_request.replacement_booking_id
    or v_p.contract_id is distinct from v_request.contract_id
    or v_p.week_ending_date is distinct from v_request.week_ending_date
    or v_p.parent_timesheet_id is distinct from v_request.root_timesheet_id
    or v_p.correction_id is distinct from v_request.correction_id
    or v_p.correction_kind is distinct from 'WHOLE_TIMESHEET_REPLACEMENT'
    or v_p.candidate_workflow_id::text is distinct from v_cap->>'workflow_id'
    or v_p.candidate_workflow_generation::text is distinct from v_cap->>'workflow_generation'
    or exists(select 1 from public.invoice_lines l where l.timesheet_id=p_timesheet_id)
    or exists(select 1 from public.timesheets_financials tf where tf.timesheet_id=p_timesheet_id
      and (tf.authorised_at_utc is not null or tf.paid_at_utc is not null or tf.locked_by_invoice_id is not null
        or tf.unlocked_by_credit_note_id is not null or tf.processed_at_utc is not null
        or exists(select 1 from jsonb_array_elements(case when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
          then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) s
          where nullif(s->>'invoice_locked_invoice_id','') is not null or nullif(s->>'invoice_locked_at_utc','') is not null
            or s->>'pay_locked'='true' or nullif(s->>'pay_locked_at_utc','') is not null)))
    or private.timesheet_correction_actor_allowed_v1((v_cap->>'actor_user_id')::uuid) is distinct from true then return null; end if;
  return v_cap;
end;
$function$;

create or replace function public.timesheet_correction_cancel_atomic_v1(
  p_environment text,p_request_id uuid,p_expected_state_revision bigint,p_actor_user_id uuid,p_now_utc timestamptz default now()
) returns jsonb language plpgsql security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment); v_request private.timesheet_correction_requests%rowtype;
  v_p public.timesheets%rowtype; v_original_before jsonb; v_original_fin_before jsonb; v_preview jsonb; v_pending jsonb;
  v_signature jsonb; v_signature_text text; v_result jsonb; v_retirement jsonb; v_receipt jsonb;
  v_week_ids uuid[]; v_preserved_ts uuid[]; v_preserved_weeks uuid[]; v_kind text;
  v_component_audit jsonb; v_retained_assets text[];
  v_opened boolean:=false; v_delete_operation uuid:=gen_random_uuid(); v_now timestamptz:=coalesce(p_now_utc,now());
begin
  perform private.timesheet_correction_actor_assert_v1(p_actor_user_id);
  select request.* into strict v_request from private.timesheet_correction_requests request where request.id=p_request_id;
  perform 1 from public.invoices i where i.id in(v_request.original_invoice_id,v_request.return_invoice_id) order by i.id for update;
  perform pg_advisory_xact_lock(hashtextextended('CORRECTION_CHAIN|'||v_request.root_timesheet_id::text,21072026));
  select request.* into strict v_request from private.timesheet_correction_requests request where request.id=p_request_id for update;
  if v_request.state<>'WAITING' then return v_request.terminal_receipt_json||jsonb_build_object('replayed',true); end if;
  if v_request.state_revision is distinct from p_expected_state_revision then
    raise exception 'CORRECTION_REQUEST_CAS_STALE' using errcode='40001';
  end if;
  -- No independent actor can retire a neighbouring workflow/week. Capture
  -- every related workflow first, then refuse anything outside exact P0.
  perform 1 from public.candidate_submission_workflows w where w.target_timesheet_id=v_request.replacement_timesheet_id
    or w.anchor_timesheet_id=v_request.replacement_timesheet_id or w.contract_week_id=v_request.replacement_week_id
    order by w.id for update;
  perform 1 from public.contract_weeks w where w.id=v_request.replacement_week_id for update;
  perform 1 from public.timesheets t where t.timesheet_id in(v_request.reversed_timesheet_id,v_request.replacement_timesheet_id)
    order by t.timesheet_id for update;
  perform 1 from public.timesheets_financials tf where tf.timesheet_id in(v_request.reversed_timesheet_id,v_request.replacement_timesheet_id)
    order by tf.timesheet_id,tf.id for update;
  select t.* into strict v_p from public.timesheets t where t.timesheet_id=v_request.replacement_timesheet_id;
  if exists(select 1 from public.candidate_submission_workflows w
    where (w.target_timesheet_id=v_p.timesheet_id or w.anchor_timesheet_id=v_p.timesheet_id or w.contract_week_id=v_request.replacement_week_id)
      and (w.id is distinct from v_p.candidate_workflow_id or w.generation is distinct from v_p.candidate_workflow_generation)) then
    raise exception 'CORRECTION_CANCEL_WORKFLOW_SCOPE_UNPROVED' using errcode='55000';
  end if;
  if to_regclass('pg_temp.timesheet_correction_cancel_capability_v1') is not null then
    raise exception 'CORRECTION_CANCEL_CONTEXT_CONFLICT' using errcode='55000';
  end if;
  create temp table pg_temp.timesheet_correction_cancel_capability_v1(
    request_id uuid primary key,timesheet_id uuid unique not null,state_revision bigint not null,
    actor_user_id uuid not null,workflow_id uuid,workflow_generation integer,contract_week_id uuid,
    delete_operation_id uuid not null,backend_pid integer not null,transaction_id bigint not null
  ) on commit drop;
  revoke all on pg_temp.timesheet_correction_cancel_capability_v1 from public,anon,authenticated,service_role;
  insert into pg_temp.timesheet_correction_cancel_capability_v1 values(v_request.id,v_p.timesheet_id,v_request.state_revision,
    p_actor_user_id,v_p.candidate_workflow_id,v_p.candidate_workflow_generation,v_request.replacement_week_id,
    v_delete_operation,pg_backend_pid(),txid_current());
  v_opened:=true;
  if private.timesheet_correction_cancel_capability_v1(v_p.timesheet_id,p_actor_user_id) is null then
    raise exception 'CORRECTION_CANCEL_P0_AUTHORITY_INVALID' using errcode='55000';
  end if;
  select to_jsonb(t) into v_original_before from public.timesheets t where t.timesheet_id=v_request.reversed_timesheet_id;
  select coalesce(jsonb_agg(to_jsonb(tf) order by tf.id),'[]'::jsonb) into v_original_fin_before
    from public.timesheets_financials tf where tf.timesheet_id=v_request.reversed_timesheet_id;
  -- Native deletion inventories TS signature keys for cleanup. Preserve the
  -- exact immutable P0 audit assets before retirement clears live TS links.
  perform 1 from public.candidate_submission_components c where c.workflow_id=v_p.candidate_workflow_id
    and c.workflow_generation=v_p.candidate_workflow_generation and c.state='IMMUTABLE' order by c.id for update;
  select coalesce(jsonb_agg(to_jsonb(c)-'timesheet_id' order by c.id),'[]'::jsonb) into v_component_audit
    from public.candidate_submission_components c where c.workflow_id=v_p.candidate_workflow_id
      and c.workflow_generation=v_p.candidate_workflow_generation and c.state='IMMUTABLE';
  select coalesce(array_agg(distinct asset_key order by asset_key),array[]::text[]) into v_retained_assets
    from public.candidate_submission_components c cross join lateral unnest(array[
      c.storage_key,c.review_storage_key,c.final_signed_storage_key,c.paper_return_page_key]) asset(asset_key)
    where c.workflow_id=v_p.candidate_workflow_id and c.workflow_generation=v_p.candidate_workflow_generation
      and c.state='IMMUTABLE' and nullif(btrim(asset_key),'') is not null;
  v_week_ids:=case when v_request.replacement_week_id is null then array[]::uuid[] else array[v_request.replacement_week_id] end;
  v_signature:=public.timesheet_lifecycle_guard_signature_v1(v_p.timesheet_id,v_request.replacement_week_id,false);
  v_signature_text:=coalesce(v_signature->>'backend_row_signature',v_signature->>'row_signature');
  v_kind:=case when v_p.sheet_scope::text='WEEKLY' then 'WEEKLY_MANUAL_ADJUSTMENT_DELETE' else 'STANDARD_DELETE' end;
  v_preview:=case when v_kind='WEEKLY_MANUAL_ADJUSTMENT_DELETE'
    then public.timesheet_weekly_manual_adjustment_delete_preview(v_p.timesheet_id,p_actor_user_id)
    else public.timesheet_standard_delete_preview_v1(v_p.timesheet_id,p_actor_user_id,v_p.timesheet_id,v_signature_text) end;
  if v_preview->'timesheet_ids' is distinct from jsonb_build_array(v_p.timesheet_id)
    or v_preview->'contract_week_ids' is distinct from to_jsonb(v_week_ids)
    or v_preview->>'decision' not in('PERMANENT_DELETE','ARCHIVE_REQUIRED') then
    raise exception 'CORRECTION_CANCEL_LAWFUL_REMOVAL_UNPROVED' using errcode='55000',detail=v_preview::text;
  end if;
  select coalesce(array_agg(id::uuid order by id),array[]::uuid[]) into v_preserved_ts
    from jsonb_array_elements_text(coalesce(v_preview->'preserved_source_timesheet_ids','[]'::jsonb)) item(id);
  select coalesce(array_agg(id::uuid order by id),array[]::uuid[]) into v_preserved_weeks
    from jsonb_array_elements_text(coalesce(v_preview->'preserved_source_contract_week_ids','[]'::jsonb)) item(id);
  v_pending:=private._timesheet_pending_expense_delete_context_v1(v_environment,array[v_p.timesheet_id],true);
  if coalesce((v_pending->>'cancellation_required')::boolean,false)
    or coalesce((v_pending->>'delete_blocked')::boolean,false) then
    raise exception 'CORRECTION_CANCEL_EXTERNAL_EXPENSE_SCOPE_UNPROVED' using errcode='55000',detail=v_pending::text;
  end if;
  if v_preview->>'decision'='ARCHIVE_REQUIRED' then
    v_retirement:=private._candidate_timesheet_delete_retire_workflows_v1(v_environment,array[v_p.timesheet_id],v_week_ids,
      array[]::uuid[],p_actor_user_id,v_delete_operation,v_now);
    v_signature:=public.timesheet_lifecycle_guard_signature_v1(v_p.timesheet_id,v_request.replacement_week_id,false);
    v_result:=public.timesheet_archive_transition_v1(v_p.timesheet_id,'ARCHIVE',v_kind,p_actor_user_id,v_p.timesheet_id,
      coalesce(v_signature->>'backend_row_signature',v_signature->>'row_signature'),v_now);
  else
    v_result:=public.timesheet_delete_with_candidate_submission_guard_apply_v1(v_environment,v_kind,v_p.timesheet_id,p_actor_user_id,
      v_p.timesheet_id,v_signature_text,array[v_p.timesheet_id],v_week_ids,array[]::uuid[],v_preserved_ts,v_preserved_weeks,
      v_pending->>'context_sha256',v_delete_operation,v_now);
  end if;
  if jsonb_typeof(v_result->'r2_cleanup_keys')='array' then
    v_result:=jsonb_set(v_result,'{r2_cleanup_keys}',coalesce((select jsonb_agg(key.value order by key.value)
      from jsonb_array_elements_text(v_result->'r2_cleanup_keys') key(value)
      where not (key.value=any(v_retained_assets))),'[]'::jsonb),true);
    if v_result?'r2_cleanup_required' then
      v_result:=jsonb_set(v_result,'{r2_cleanup_required}',to_jsonb(jsonb_array_length(v_result->'r2_cleanup_keys')>0),true);
    end if;
  end if;
  if v_result->>'ok'='false' or v_result->>'success'='false'
    or exists(select 1 from public.timesheets t where t.timesheet_id=v_p.timesheet_id and t.archived_at_utc is null)
    or exists(select 1 from public.timesheets t where t.booking_id=v_request.replacement_booking_id
      and t.timesheet_id<>v_request.replacement_timesheet_id and t.is_current and t.archived_at_utc is null)
    or (v_preview->>'decision'='PERMANENT_DELETE' and exists(select 1 from public.contract_weeks w where w.id=v_request.replacement_week_id))
    or (v_preview->>'decision'='ARCHIVE_REQUIRED' and v_request.replacement_week_id is not null
      and not exists(select 1 from public.contract_weeks w join public.timesheets t on t.timesheet_id=w.timesheet_id
        where w.id=v_request.replacement_week_id and w.timesheet_id=v_request.replacement_timesheet_id and t.archived_at_utc is not null))
    or exists(select 1 from public.candidate_submission_workflows w where w.id=v_p.candidate_workflow_id
      and (w.target_timesheet_id is not null or w.anchor_timesheet_id is not null or w.contract_week_id is not null or w.state<>'CANCELLED'))
    or exists(select 1 from public.candidate_approval_requests a where a.workflow_id=v_p.candidate_workflow_id and a.state='PENDING')
    or (select coalesce(jsonb_agg(to_jsonb(c)-'timesheet_id' order by c.id),'[]'::jsonb)
      from public.candidate_submission_components c where c.workflow_id=v_p.candidate_workflow_id
        and c.workflow_generation=v_p.candidate_workflow_generation and c.state='IMMUTABLE') is distinct from v_component_audit
    or (select to_jsonb(t) from public.timesheets t where t.timesheet_id=v_request.reversed_timesheet_id) is distinct from v_original_before
    or (select coalesce(jsonb_agg(to_jsonb(tf) order by tf.id),'[]'::jsonb) from public.timesheets_financials tf
      where tf.timesheet_id=v_request.reversed_timesheet_id) is distinct from v_original_fin_before then
    raise exception 'CORRECTION_CANCEL_COMPLETE_UNIT_FAILED' using errcode='55000',detail=v_result::text;
  end if;
  v_receipt:=jsonb_build_object('ok',true,'state','CANCELLED','request_id',v_request.id,'state_revision',v_request.state_revision+1,
    'replacement_timesheet_id',v_p.timesheet_id,'delete_operation_id',v_delete_operation,'lawful_removal',v_result,
    'retained_immutable_component_asset_count',cardinality(v_retained_assets));
  update private.timesheet_correction_requests request set state='CANCELLED',state_revision=state_revision+1,
    cancelled_at_utc=v_now,updated_at_utc=v_now,terminal_receipt_json=v_receipt where request.id=v_request.id;
  drop table pg_temp.timesheet_correction_cancel_capability_v1;
  v_opened:=false;
  return v_receipt;
exception when others then
  if v_opened then drop table if exists pg_temp.timesheet_correction_cancel_capability_v1; end if;
  raise;
end;
$function$;
