-- Candidate-safe bootstrap, Contract Timesheets reads and missing-week authority.

create or replace function private._candidate_rejection_replaced_v1(
  p_rejected_workflow_id uuid
)
returns boolean
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_rejected public.candidate_submission_workflows%rowtype;
begin
  if p_rejected_workflow_id is null then
    return false;
  end if;

  select workflow.* into v_rejected
  from public.candidate_submission_workflows workflow
  where workflow.id=p_rejected_workflow_id;

  if not found or v_rejected.state<>'REJECTED' then
    return false;
  end if;

  -- Direct replacement lineage is durable historical truth.  A successor
  -- remains the replacement even if it is later cancelled, expires or is
  -- superseded; the original rejected workflow must never advertise a second
  -- impossible direct resubmission.
  if exists(
    select 1
    from public.candidate_submission_workflows direct_replacement
    where direct_replacement.replacement_of_workflow_id=v_rejected.id
  ) then
    return true;
  end if;

  return exists(
    select 1
    from public.candidate_submission_workflows later
    where later.candidate_id=v_rejected.candidate_id
      and later.contract_id is not distinct from v_rejected.contract_id
      and later.id<>v_rejected.id
      and later.state not in ('CANCELLED','EXPIRED','SUPERSEDED')
      and later.created_at_utc>=v_rejected.updated_at_utc
      and (
        (
          (
            v_rejected.workflow_kind='CONTRACT_EXPENSE'
            or v_rejected.rejection_scope='COMPLETE_EXPENSE_CLAIM'
          )
          and later.week_ending_date is not distinct from v_rejected.week_ending_date
          and later.workflow_kind in ('CONTRACT_EXPENSE','CONTRACT_COMBINED')
        )
        or (
          v_rejected.workflow_kind='CONTRACT_COMBINED'
          and later.workflow_kind='CONTRACT_COMBINED'
          and later.contract_week_id is not distinct from v_rejected.contract_week_id
        )
        or (
          v_rejected.workflow_kind='CONTRACT_HOURS'
          and later.workflow_kind in ('CONTRACT_HOURS','CONTRACT_COMBINED')
          and later.contract_week_id is not distinct from v_rejected.contract_week_id
        )
        or (
          v_rejected.workflow_kind='DAILY'
          and later.workflow_kind='DAILY'
          and later.work_date is not distinct from v_rejected.work_date
          and exists(
            select 1
            from public.timesheets rejected_timesheet
            join public.timesheets later_timesheet
              on nullif(btrim(coalesce(later_timesheet.booking_id,'')),'')
                =nullif(btrim(coalesce(rejected_timesheet.booking_id,'')),'')
            where rejected_timesheet.timesheet_id=coalesce(
                v_rejected.target_timesheet_id,v_rejected.anchor_timesheet_id
              )
              and later_timesheet.timesheet_id=coalesce(
                later.target_timesheet_id,later.anchor_timesheet_id
              )
              and nullif(btrim(coalesce(rejected_timesheet.booking_id,'')),'') is not null
          )
        )
      )
  );
end;
$function$;

create or replace function private._candidate_status_code_v1(
  p_paid boolean,
  p_authorised boolean,
  p_invoiced_not_paid boolean,
  p_active_workflow_state text,
  p_actionable_rejection boolean,
  p_processing_status text,
  p_contract_week_status text
)
returns text
language sql
immutable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
  select case
    when coalesce(p_paid,false) then 'PAID'
    when coalesce(p_authorised,false) then 'AUTHORISED'
    when coalesce(p_invoiced_not_paid,false) then 'INVOICED_NOT_PAID'
    when nullif(btrim(coalesce(p_active_workflow_state,'')),'') is not null
      then upper(btrim(p_active_workflow_state))
    when coalesce(p_actionable_rejection,false) then 'REJECTED'
    when nullif(btrim(coalesce(p_processing_status,'')),'') is not null
      then upper(btrim(p_processing_status))
    else upper(coalesce(nullif(btrim(p_contract_week_status),''),'AVAILABLE'))
  end;
$function$;

create or replace function public.candidate_app_bootstrap_v1(
  p_session_id uuid,
  p_environment text,
  p_expected_rotation integer,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_context jsonb;
  v_candidate public.candidates%rowtype;
  v_flags jsonb;
  v_contract_enabled boolean:=false;
begin
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_reads');
  v_context:=private._candidate_session_context_v1(p_session_id,p_environment,p_expected_rotation,p_now_utc,false);
  if nullif(v_context->>'selected_candidate_id','') is not null then
    select * into v_candidate from public.candidates where id=(v_context->>'selected_candidate_id')::uuid;
    select exists(
      select 1
      from public.contract_weeks cw
      join public.contracts c on c.id=cw.contract_id and c.candidate_id=v_candidate.id
      left join lateral (
        select cs.week_ending_weekday
        from public.client_settings cs
        where cs.client_id=c.client_id
          and cs.effective_from<=(p_now_utc at time zone 'Europe/London')::date
        order by cs.effective_from desc,cs.updated_at desc nulls last,cs.id desc
        limit 1
      ) effective_client on true
      cross join lateral (
        select (
          (p_now_utc at time zone 'Europe/London')::date
          + mod(
              coalesce(c.week_ending_weekday_snapshot,effective_client.week_ending_weekday,0)
              - extract(dow from (p_now_utc at time zone 'Europe/London')::date)::integer
              + 7,
              7
            )
        )::date as current_week_ending_date
      ) entitlement_window
      left join public.timesheets t on t.timesheet_id=cw.timesheet_id
        and t.is_current=true and t.archived_at_utc is null
      left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current=true
      where cw.week_ending_date >= (entitlement_window.current_week_ending_date-interval '6 months')::date
        and cw.week_ending_date <= entitlement_window.current_week_ending_date
        and (
          cw.status in ('OPEN','SUBMITTED','AUTHORISED','INVOICED')
          or tf.processing_status in (
            'UNASSIGNED','CLIENT_UNRESOLVED','RATE_MISSING','PAY_CHANNEL_MISSING',
            'READY_FOR_HR','READY_FOR_INVOICE','PENDING_AUTH',
            'AWAITING_MANUAL_SIGNATURE','UNPROCESSED'
          )
          or cw.week_ending_date=entitlement_window.current_week_ending_date
        )
    ) into v_contract_enabled;
  end if;
  select candidate_app_feature_flags_json into v_flags from public.settings_defaults where id=1;
  return jsonb_build_object(
    'ok',true,
    'feature_contract_version','candidate-app-private-v1',
    'environment',v_context->>'environment',
    'account_id',v_context->'account_id',
    'selected_candidate_id',v_context->'selected_candidate_id',
    'selection_required',coalesce((v_context#>>'{eligibility,selection_required}')::boolean,false)
      and (v_context->>'selected_candidate_id') is null,
    'selectable_candidate_ids',case
      when upper(v_context->>'environment')='TEST' then v_context#>'{eligibility,candidate_ids}'
      else '[]'::jsonb end,
    'entitlements',jsonb_build_object(
      'contract',v_contract_enabled,
      'daily',nullif(btrim(coalesce(v_candidate.key_norm,'')),'') is not null,
      'gck_present',nullif(btrim(coalesce(v_candidate.key_norm,'')),'') is not null
    ),
    'notification_preferences',v_context->'notification_preferences',
    'feature_flags',coalesce(v_flags,'{}'::jsonb),
    'session',jsonb_build_object(
      'rotation',(v_context->>'rotation')::integer,
      'session_version',(v_context->>'session_version')::bigint,
      'expires_at_utc',v_context->'expires_at_utc',
      'absolute_expires_at_utc',v_context->'absolute_expires_at_utc'
    )
  );
end;
$function$;

create or replace function private._candidate_week_ending_label_v1(p_date date)
returns text
language sql
immutable
set search_path = pg_catalog, public, private, pg_temp
as $function$
  select case when p_date is null then null else
    'Week Ending '||extract(day from p_date)::integer::text||' '
    ||(array['January','February','March','April','May','June','July','August',
              'September','October','November','December'])[extract(month from p_date)::integer]
    ||' '||extract(year from p_date)::integer::text end
$function$;

create or replace function private._candidate_workflow_maps_to_card_v1(
  p_workflow_id uuid,
  p_timesheet_id uuid,
  p_contract_week_id uuid
)
returns boolean
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_card_timesheet public.timesheets%rowtype;
  v_anchor public.timesheets%rowtype;
  v_target public.timesheets%rowtype;
  v_parent public.timesheets%rowtype;
begin
  if p_workflow_id is null or p_contract_week_id is null then return false; end if;
  select * into v_workflow from public.candidate_submission_workflows where id=p_workflow_id;
  if not found then return false; end if;
  if v_workflow.contract_week_id=p_contract_week_id then return true; end if;
  if p_timesheet_id is null
     or not (v_workflow.workflow_kind='CONTRACT_EXPENSE'
       or v_workflow.rejection_scope='COMPLETE_EXPENSE_CLAIM') then return false; end if;
  select * into v_card_timesheet from public.timesheets where timesheet_id=p_timesheet_id;
  if not found then return false; end if;
  if v_workflow.anchor_timesheet_id is not null then
    select * into v_anchor from public.timesheets where timesheet_id=v_workflow.anchor_timesheet_id;
    if found and (
      v_anchor.timesheet_id=v_card_timesheet.timesheet_id
      or (nullif(btrim(coalesce(v_anchor.booking_id,'')),'') is not null
        and v_anchor.booking_id=v_card_timesheet.booking_id
        and v_anchor.contract_id is not distinct from v_card_timesheet.contract_id
        and v_anchor.week_ending_date is not distinct from v_card_timesheet.week_ending_date)
    ) then return true; end if;
  end if;
  if v_workflow.target_timesheet_id is not null then
    select * into v_target from public.timesheets where timesheet_id=v_workflow.target_timesheet_id;
    if found and v_target.parent_timesheet_id is not null then
      select * into v_parent from public.timesheets where timesheet_id=v_target.parent_timesheet_id;
      if found and (
        v_parent.timesheet_id=v_card_timesheet.timesheet_id
        or (nullif(btrim(coalesce(v_parent.booking_id,'')),'') is not null
          and v_parent.booking_id=v_card_timesheet.booking_id
          and v_parent.contract_id is not distinct from v_card_timesheet.contract_id
          and v_parent.week_ending_date is not distinct from v_card_timesheet.week_ending_date)
      ) then return true; end if;
    end if;
  end if;
  return false;
end;
$function$;

create or replace function private._candidate_paper_pack_readiness_v1(
  p_workflow_id uuid,
  p_expected_generation integer
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_mail public.mail_outbox%rowtype;
  v_count integer:=0;
  v_manifest text;
  v_scope jsonb;
  v_attachment jsonb;
  v_state text:='PREPARING';
  v_reason text:='CANDIDATE_PAPER_PACK_PREPARING';
  v_ready boolean:=false;
  v_retryable boolean:=false;
  v_failure_scope text;
  v_failure_code text;
  v_attempt_count integer:=0;
  v_next_retry_at timestamptz;
  v_retry_in_progress boolean:=false;
  v_operation_id text;
begin
  select * into v_workflow from public.candidate_submission_workflows
  where id=p_workflow_id and generation=p_expected_generation;
  if not found or v_workflow.route<>'PAPER' then
    return jsonb_build_object('state','NOT_APPLICABLE','download_available',false,
      'upload_available',false,'page_count',null,'reason_code','CANDIDATE_PAPER_PACK_NOT_APPLICABLE');
  end if;
  if v_workflow.state<>'AWAITING_PAPER_RETURN' then
    return jsonb_build_object('state',case when v_workflow.state='RECEIVED' then 'RETURN_RECEIVED' else 'NOT_APPLICABLE' end,
      'download_available',false,'upload_available',false,'page_count',null,
      'reason_code',case when v_workflow.state='RECEIVED' then 'CANDIDATE_PAPER_RETURN_RECEIVED'
        else 'CANDIDATE_PAPER_PACK_NOT_APPLICABLE' end);
  end if;
  if coalesce(v_workflow.last_mutation_response_json->>'failure_scope','')='WORKFLOW'
     and coalesce(v_workflow.last_mutation_response_json->>'paper_pack_state','')='FAILED_TERMINAL' then
    return jsonb_build_object(
      'state','FAILED_TERMINAL','download_available',false,'upload_available',false,
      'page_count',null,'reason_code',coalesce(
        v_workflow.last_mutation_response_json->>'failure_code',
        'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED'
      ),
      'failure_scope','WORKFLOW','failure_code',coalesce(
        v_workflow.last_mutation_response_json->>'failure_code',
        'CANDIDATE_PAPER_PACK_OPERATIONAL_REVIEW_REQUIRED'
      ),
      'retryable',false,'attempt_count',0,'next_retry_at_utc',null,
      'retry_in_progress',false,
      'operation_id',v_workflow.last_mutation_response_json->>'paper_pack_operation_id'
    );
  end if;
  v_manifest:=case when v_workflow.paper_return_manifest_sha256 is null then null
    else encode(v_workflow.paper_return_manifest_sha256,'hex') end;
  if v_manifest is null then
    return jsonb_build_object('state',v_state,'download_available',false,'upload_available',false,
      'page_count',null,'reason_code',v_reason);
  end if;
  select count(*)::integer into v_count
  from public.mail_outbox mail
  where mail.type='TIMESHEET_QR' and mail.context_kind='timesheets'
    and mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
    and mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
    and lower(coalesce(mail.payment_scope_json->>'paper_return_manifest_sha256',''))=v_manifest;
  if v_count>1 then
    return jsonb_build_object('state','STALE','download_available',false,'upload_available',false,
      'page_count',null,'reason_code','CANDIDATE_PAPER_OUTBOX_CONFLICT');
  elsif v_count=0 then
    return jsonb_build_object('state',v_state,'download_available',false,'upload_available',false,
      'page_count',null,'reason_code',v_reason);
  end if;
  select mail.* into v_mail from public.mail_outbox mail
  where mail.type='TIMESHEET_QR' and mail.context_kind='timesheets'
    and mail.payment_scope_json->>'candidate_workflow_id'=v_workflow.id::text
    and mail.payment_scope_json->>'candidate_workflow_generation'=v_workflow.generation::text
    and lower(coalesce(mail.payment_scope_json->>'paper_return_manifest_sha256',''))=v_manifest;
  v_scope:=coalesce(v_mail.payment_scope_json,'{}'::jsonb);
  v_failure_scope:='OUTBOX';
  v_failure_code:=nullif(v_scope->>'candidate_paper_pack_failure_code','');
  v_retryable:=lower(coalesce(v_scope->>'candidate_paper_pack_retryable','false'))
    in ('true','t','1','yes');
  v_attempt_count:=case when coalesce(v_scope->>'candidate_paper_pack_attempt_count','')~'^[0-9]+$'
    then (v_scope->>'candidate_paper_pack_attempt_count')::integer else 0 end;
  v_next_retry_at:=nullif(v_scope->>'candidate_paper_pack_next_retry_at_utc','')::timestamptz;
  v_retry_in_progress:=coalesce(
    nullif(v_scope->>'candidate_paper_pack_attempt_token','') is not null
      and nullif(v_scope->>'candidate_paper_pack_attempt_expires_at_utc','')::timestamptz>now(),
    false
  );
  v_operation_id:=nullif(v_scope->>'candidate_paper_pack_operation_id','');
  if lower(coalesce(v_scope->>'candidate_paper_generation_retired','false')) in ('true','t','1','yes') then
    v_state:='RETIRED'; v_reason:='CANDIDATE_PAPER_GENERATION_RETIRED';
  elsif v_retryable then
    if v_next_retry_at is not null and v_next_retry_at>now() then
      v_state:='BACKOFF'; v_reason:='CANDIDATE_PAPER_PACK_RETRY_BACKOFF_ACTIVE';
    else
      v_state:='FAILED_RETRYABLE';
      v_reason:=coalesce(v_failure_code,'CANDIDATE_PAPER_PACK_FAILED_RETRYABLE');
    end if;
  elsif upper(coalesce(v_scope->>'candidate_paper_pack_failure_class',''))='TERMINAL' then
    v_state:='FAILED_TERMINAL';
    v_reason:=coalesce(v_failure_code,'CANDIDATE_PAPER_PACK_FAILED_TERMINAL');
  elsif v_mail.status='FAILED' then
    v_state:='FAILED_TERMINAL'; v_reason:='CANDIDATE_PAPER_OUTBOX_FAILED';
  elsif jsonb_typeof(v_mail.attachments)='array' and jsonb_array_length(v_mail.attachments)=1 then
    v_attachment:=v_mail.attachments->0;
    v_ready:=v_mail.status in ('QUEUED','SENT')
      and lower(coalesce(v_scope->>'candidate_paper_pack_ready','false')) in ('true','t','1','yes')
      and lower(coalesce(v_scope->>'mail_held_until_pdf_rendered','false')) in ('false','f','0','no')
      and nullif(btrim(coalesce(v_scope->>'mail_hold_reason','')),'') is null
      and v_attachment->>'r2_key'=v_scope->>'candidate_complete_pack_storage_key'
      and lower(coalesce(v_attachment->>'sha256',''))=lower(coalesce(v_scope->>'candidate_complete_pack_sha256',''))
      and v_attachment->>'size_bytes'=v_scope->>'candidate_complete_pack_size_bytes'
      and v_attachment->>'page_count'=v_scope->>'candidate_complete_pack_page_count'
      and lower(coalesce(v_attachment->>'content_type',''))='application/pdf'
      and v_attachment->>'candidate_workflow_id'=v_workflow.id::text
      and v_attachment->>'candidate_workflow_generation'=v_workflow.generation::text
      and lower(coalesce(v_attachment->>'paper_return_manifest_sha256',''))=v_manifest;
    if v_ready then v_state:='READY'; v_reason:=null; end if;
  end if;
  return jsonb_build_object('state',v_state,'download_available',v_ready,'upload_available',v_ready,
    'page_count',case when v_ready then nullif(v_scope->>'candidate_complete_pack_page_count','')::integer else null end,
    'reason_code',v_reason,'failure_scope',v_failure_scope,'failure_code',v_failure_code,
    'retryable',v_retryable,'attempt_count',v_attempt_count,
    'next_retry_at_utc',v_next_retry_at,'retry_in_progress',v_retry_in_progress,
    'operation_id',v_operation_id);
end;
$function$;

create or replace function private._candidate_action_invocation_v1(p_action jsonb)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_code text:=upper(coalesce(p_action->>'code',''));
  v_method text:=upper(coalesce(p_action->>'method',''));
  v_week public.contract_weeks%rowtype;
  v_route_family text;
  v_fixed jsonb:='{}'::jsonb;
  v_inputs jsonb:='[]'::jsonb;
  v_invocation jsonb;
begin
  if v_code in ('CONTINUE_TIMESHEET','CONTINUE_EXPENSE_CLAIM') then
    v_invocation:=jsonb_build_object('version',1,'kind','CLIENT_DESTINATION',
      'destination',case when v_code='CONTINUE_EXPENSE_CLAIM' then 'EXPENSE_CLAIM_EDITOR' else 'TIMESHEET_EDITOR' end,
      'context',jsonb_build_object('workflow_id',p_action->'workflow_id',
        'workflow_generation',p_action->'workflow_generation'));
    return (p_action-'method'-'path')||jsonb_build_object('method',null,'path',null,'invocation',v_invocation);
  end if;
  if v_code in ('ENTER_TIMESHEET','ADD_EXPENSES') then
    select * into v_week from public.contract_weeks where id=nullif(p_action->>'contract_week_id','')::uuid;
    if not found then return p_action||jsonb_build_object('enabled',false,
      'disabled_reason','CANDIDATE_ACTION_CONTEXT_STALE'); end if;
    v_route_family:=private._candidate_route_family_v1(v_week.timesheet_id,v_week.id)->>'route_family';
    v_fixed:=jsonb_build_object('workflow',jsonb_strip_nulls(jsonb_build_object(
      'workflow_kind',case when v_code='ADD_EXPENSES' then 'CONTRACT_EXPENSE' else 'CONTRACT_HOURS' end,
      'scope','WEEKLY','route',case when v_route_family='QR' then 'PAPER' else 'ELECTRONIC' end,
      'contract_id',v_week.contract_id,'contract_week_id',v_week.id,
      'week_ending_date',v_week.week_ending_date,
      'anchor_timesheet_id',case when v_code='ADD_EXPENSES' then p_action->'timesheet_id' else null end
    )));
    v_inputs:='[{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb;
  elsif v_code in ('RESUBMIT_TIMESHEET','RESUBMIT_TIMESHEET_AND_EXPENSES','RESUBMIT_EXPENSE_CLAIM','REVIEW_AND_RESUBMIT') then
    v_fixed:=jsonb_build_object('generation',p_action->'workflow_generation');
    v_inputs:='[{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb;
  elsif v_method='POST' then
    if p_action ? 'workflow_generation' then
      v_fixed:=jsonb_build_object('generation',p_action->'workflow_generation');
    end if;
    v_inputs:=case v_code
      when 'DISCARD_EXPENSE_CLAIM' then '[{"name":"reason_note","type":"string","required":true},{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb
      when 'CANCEL_ENTIRE_CLAIM_AND_START_AGAIN' then '[{"name":"reason_note","type":"string","required":true},{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb
      when 'UPLOAD_SIGNED_RETURN' then '[{"name":"returned_pages","type":"array","required":true},{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb
      when 'NO_WORK_THIS_WEEK' then '[{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb
      else '[{"name":"idempotency_key","type":"uuid","required":true}]'::jsonb end;
  end if;
  v_invocation:=jsonb_build_object('version',1,'kind','HTTP','method',nullif(v_method,''),
    'path',p_action->>'path','fixed_body',v_fixed,'required_user_inputs',v_inputs,
    'idempotency',case when v_method='POST' then 'REQUIRED' else 'NOT_REQUIRED' end);
  return p_action||jsonb_build_object('invocation',v_invocation);
end;
$function$;

create or replace function private._candidate_timesheet_primary_action_v1(
  p_candidate_status_code text,
  p_workflows jsonb,
  p_capabilities jsonb,
  p_timesheet_id uuid,
  p_contract_week_id uuid
)
returns jsonb
language plpgsql
stable
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow jsonb;
  v_action text;
begin
  if upper(coalesce(p_candidate_status_code,'')) in ('PAID','AUTHORISED','INVOICED_NOT_PAID') then
    return null;
  end if;
  select item into v_workflow
  from jsonb_array_elements(coalesce(p_workflows,'[]'::jsonb)) item
  where item->>'state'='REJECTED'
    and coalesce((item->>'rejection_actionable')::boolean,false)
    and nullif(item->>'required_resubmission_action','') is not null
  order by coalesce((item->>'detail_action_owner')::boolean,false) desc,
    item->>'updated_at_utc' desc,item->>'workflow_id'
  limit 1;
  if v_workflow is not null then
    v_action:=v_workflow->>'required_resubmission_action';
    return jsonb_build_object(
      'code',v_action,
      'label',case v_action
        when 'RESUBMIT_EXPENSE_CLAIM' then 'Resubmit Expense Claim'
        when 'RESUBMIT_TIMESHEET_AND_EXPENSES' then 'Resubmit Timesheet and Expenses'
        else 'Resubmit Timesheet' end,
      'method','POST',
      'path','/candidate-app/v1/workflows/'||(v_workflow->>'workflow_id')||'/resubmit',
      'requires_confirmation',false,'requires_reason',false,
      'enabled',true,'disabled_reason',null,
      'workflow_id',v_workflow->>'workflow_id',
      'workflow_generation',nullif(v_workflow->>'generation','')::integer
    );
  end if;
  select item into v_workflow
  from jsonb_array_elements(coalesce(p_workflows,'[]'::jsonb)) item
  where item->>'state' in (
    'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
    'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
    'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
    'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
    'AWAITING_PAPER_RETURN','RECEIVED','REFUSED'
  )
  order by coalesce((item->>'detail_action_owner')::boolean,false) desc,
    item->>'updated_at_utc' desc,item->>'workflow_id'
  limit 1;
  if v_workflow is not null then
    v_action:=case
      when v_workflow->>'state'='REFUSED' then 'REVIEW_AND_RESUBMIT'
      when v_workflow->>'workflow_kind'='CONTRACT_EXPENSE' then 'CONTINUE_EXPENSE_CLAIM'
      else 'CONTINUE_TIMESHEET' end;
    return jsonb_build_object(
      'code',v_action,
      'label',case v_action
        when 'REVIEW_AND_RESUBMIT' then 'Review and Resubmit'
        when 'CONTINUE_EXPENSE_CLAIM' then 'Continue Expense Claim'
        else 'Continue Timesheet' end,
      'method',case when v_action='REVIEW_AND_RESUBMIT' then 'POST' else 'GET' end,
      'path',case when v_action='REVIEW_AND_RESUBMIT'
        then '/candidate-app/v1/workflows/'||(v_workflow->>'workflow_id')||'/actions/amend'
        else '/candidate-app/v1/workflows/'||(v_workflow->>'workflow_id')||'/timesheet-detail' end,
      'requires_confirmation',false,'enabled',true,'disabled_reason',null,
      'workflow_id',v_workflow->>'workflow_id',
      'workflow_generation',nullif(v_workflow->>'generation','')::integer
    );
  end if;
  if coalesce((p_capabilities->>'can_edit_hours')::boolean,false) then
    return jsonb_build_object(
      'code','ENTER_TIMESHEET','label','Enter Timesheet',
      'method','POST','path','/candidate-app/v1/workflows',
      'requires_confirmation',false,'requires_reason',false,
      'enabled',true,'disabled_reason',null,'timesheet_id',p_timesheet_id,
      'contract_week_id',p_contract_week_id
    );
  end if;
  if coalesce((p_capabilities->>'can_edit_expenses')::boolean,false) then
    return jsonb_build_object(
      'code','ADD_EXPENSES','label','Add Expenses',
      'method','POST','path','/candidate-app/v1/workflows',
      'requires_confirmation',false,'requires_reason',false,
      'enabled',true,'disabled_reason',null,'timesheet_id',p_timesheet_id,
      'contract_week_id',p_contract_week_id
    );
  end if;
  return null;
end;
$function$;

create or replace function private._candidate_timesheet_action_contract_v1(
  p_candidate_status_code text,
  p_workflows jsonb,
  p_capabilities jsonb,
  p_timesheet_id uuid,
  p_contract_week_id uuid,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_primary jsonb;
  v_actions jsonb:='[]'::jsonb;
  v_workflow_json jsonb;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_approval public.candidate_approval_requests%rowtype;
  v_manager_approval jsonb;
  v_provider_first_accepted_at timestamptz;
  v_provider_accepted_at timestamptz;
  v_latest_delivery_state text;
  v_latest_provider_status text;
  v_pending_delivery_count integer:=0;
  v_next_reminder_at timestamptz;
  v_reminder_eligible boolean:=false;
  v_renewal_eligible boolean:=false;
  v_cancel_eligible boolean:=false;
  v_effective_request_state text;
  v_disabled_reason text;
  v_action jsonb;
  v_cancel_code text;
  v_rejection jsonb;
  v_paper_pack jsonb;
begin
  v_primary:=private._candidate_timesheet_primary_action_v1(
    p_candidate_status_code,p_workflows,p_capabilities,p_timesheet_id,p_contract_week_id
  );
  if v_primary is not null then v_actions:=jsonb_build_array(v_primary); end if;

  for v_rejection in
    select item
    from jsonb_array_elements(coalesce(p_workflows,'[]'::jsonb)) item
    where item->>'state'='REJECTED'
      and coalesce((item->>'rejection_actionable')::boolean,false)
      and nullif(item->>'required_resubmission_action','') is not null
    order by coalesce((item->>'detail_action_owner')::boolean,false) desc,
      item->>'updated_at_utc' desc,item->>'workflow_id'
  loop
    if not exists(
      select 1 from jsonb_array_elements(v_actions) existing
      where existing->>'workflow_id'=v_rejection->>'workflow_id'
        and existing->>'code'=v_rejection->>'required_resubmission_action'
    ) then
      v_action:=jsonb_build_object(
        'code',v_rejection->>'required_resubmission_action',
        'label',case v_rejection->>'required_resubmission_action'
          when 'RESUBMIT_EXPENSE_CLAIM' then 'Resubmit Expense Claim'
          when 'RESUBMIT_TIMESHEET_AND_EXPENSES' then 'Resubmit Timesheet and Expenses'
          else 'Resubmit Timesheet' end,
        'method','POST','path','/candidate-app/v1/workflows/'
          ||(v_rejection->>'workflow_id')||'/resubmit',
        'requires_confirmation',false,'requires_reason',false,'enabled',true,
        'disabled_reason',null,'workflow_id',v_rejection->>'workflow_id',
        'workflow_generation',nullif(v_rejection->>'generation','')::integer,
        'claim_family',v_rejection->>'claim_family'
      );
      v_actions:=v_actions||jsonb_build_array(v_action);
    end if;
  end loop;

  select item into v_workflow_json
  from jsonb_array_elements(coalesce(p_workflows,'[]'::jsonb)) item
  where item->>'state' in (
    'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
    'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
    'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
    'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
    'AWAITING_PAPER_RETURN','RECEIVED','REFUSED'
  )
  order by coalesce((item->>'detail_action_owner')::boolean,false) desc,
    item->>'updated_at_utc' desc,item->>'workflow_id'
  limit 1;

  if v_workflow_json is not null then
    select workflow_row.* into v_workflow
    from public.candidate_submission_workflows workflow_row
    where workflow_row.id=(v_workflow_json->>'workflow_id')::uuid;
  end if;

  if v_workflow.id is not null then
    v_cancel_eligible:=v_workflow.state not in (
      'FINALISED','CANCELLED','REJECTED','SUPERSEDED','EXPIRED'
    );
    v_cancel_code:=case when v_workflow.workflow_kind='CONTRACT_EXPENSE'
      then 'DISCARD_EXPENSE_CLAIM' else 'CANCEL_ENTIRE_CLAIM_AND_START_AGAIN' end;
    if v_cancel_eligible then
      v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
        'code',v_cancel_code,
        'label',case when v_cancel_code='DISCARD_EXPENSE_CLAIM'
          then 'Discard Expense Claim' else 'Cancel Entire Claim and Start Again' end,
        'method','POST','path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/cancel',
        'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
        'approval_request_id',null,'requires_confirmation',true,'requires_reason',true,
        'enabled',true,'disabled_reason',null
      ));
    end if;

    if v_workflow.route='EMAIL' and v_workflow.state='AWAITING_MANAGER_APPROVAL' then
      select request_row.* into v_approval
      from public.candidate_approval_requests request_row
      where request_row.workflow_id=v_workflow.id
        and request_row.workflow_generation=v_workflow.generation
        and request_row.method='EMAIL'
      order by request_row.request_generation desc,request_row.created_at_utc desc
      limit 1;
      if found then
        select
          min(delivery.sent_at) filter (
            where delivery.status='SENT' and delivery.sent_at is not null
              and upper(coalesce(delivery.provider_status,'')) in ('ACCEPTED','SENT','SUCCESS','OK')
          ),
          max(delivery.sent_at) filter (
            where delivery.status='SENT' and delivery.sent_at is not null
              and upper(coalesce(delivery.provider_status,'')) in ('ACCEPTED','SENT','SUCCESS','OK')
          ),
          count(*) filter (
            where delivery.status='QUEUED' and delivery.sent_at is null
              and lower(coalesce(delivery.payment_scope_json->>'candidate_manager_mail_retired','false'))
                    in ('false','f','0','no')
          )::integer
        into v_provider_first_accepted_at,v_provider_accepted_at,v_pending_delivery_count
        from public.mail_outbox delivery
        where upper(coalesce(delivery.payment_scope_json->>'candidate_mail_authority',''))
                ='MANAGER_APPROVAL_V1'
          and delivery.payment_scope_json->>'candidate_manager_workflow_id'=v_workflow.id::text
          and delivery.payment_scope_json->>'candidate_manager_workflow_generation'=v_workflow.generation::text
          and delivery.payment_scope_json->>'candidate_approval_request_id'=v_approval.id::text
          and delivery.payment_scope_json->>'candidate_approval_request_generation'=v_approval.request_generation::text
          and upper(coalesce(delivery.payment_scope_json->>'candidate_manager_mail_kind',''))
                in ('INITIAL','REMINDER','RENEWAL');
        select delivery.status,delivery.provider_status
        into v_latest_delivery_state,v_latest_provider_status
        from public.mail_outbox delivery
        where upper(coalesce(delivery.payment_scope_json->>'candidate_mail_authority',''))
                ='MANAGER_APPROVAL_V1'
          and delivery.payment_scope_json->>'candidate_approval_request_id'=v_approval.id::text
          and upper(coalesce(delivery.payment_scope_json->>'candidate_manager_mail_kind',''))
                in ('INITIAL','REMINDER','RENEWAL')
        order by delivery.created_at_utc desc,delivery.id desc limit 1;

        v_effective_request_state:=case
          when v_approval.state='PENDING' and v_approval.expires_at_utc<=p_now_utc then 'EXPIRED'
          else v_approval.state end;
        v_next_reminder_at:=case when v_provider_accepted_at is null then null
          else v_provider_accepted_at+interval '24 hours' end;
        v_reminder_eligible:=v_effective_request_state='PENDING'
          and v_provider_accepted_at is not null
          and v_next_reminder_at<=p_now_utc
          and v_approval.resend_count<5
          and v_pending_delivery_count=0;
        v_renewal_eligible:=v_effective_request_state='EXPIRED'
          and v_approval.review_manifest_sha256 is not distinct from v_workflow.review_manifest_sha256;
        v_disabled_reason:=case
          when v_effective_request_state<>'PENDING' then 'MANAGER_APPROVAL_REQUEST_NOT_PENDING'
          when v_provider_accepted_at is null then 'MANAGER_EMAIL_NOT_PROVIDER_ACCEPTED'
          when v_pending_delivery_count>0 then 'MANAGER_EMAIL_DELIVERY_PENDING'
          when v_approval.resend_count>=5 then 'MANAGER_REMINDER_LIMIT_REACHED'
          when v_next_reminder_at>p_now_utc then 'MANAGER_REMINDER_WAIT_24_HOURS'
          else null end;
        v_manager_approval:=jsonb_build_object(
          'method','EMAIL','request_id',v_approval.id,
          'request_generation',v_approval.request_generation,
          'state',v_effective_request_state,
          'stored_state',v_approval.state,
          'provider_first_accepted_at_utc',v_provider_first_accepted_at,
          'provider_accepted_at_utc',v_provider_accepted_at,
          'delivery_state',v_latest_delivery_state,
          'provider_status',v_latest_provider_status,
          'delivery_pending',v_pending_delivery_count>0,
          'expires_at_utc',v_approval.expires_at_utc,
          'resend_count',v_approval.resend_count,
          'resends_remaining',greatest(5-v_approval.resend_count,0),
          'next_reminder_at_utc',v_next_reminder_at,
          'reminder_eligible',v_reminder_eligible,
          'renewal_eligible',v_renewal_eligible,
          'cancel_eligible',v_cancel_eligible
        );
        v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
          'code','SEND_MANAGER_REMINDER','label','Send Manager Reminder',
          'method','POST','path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/remind',
          'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
          'approval_request_id',v_approval.id,
          'approval_request_generation',v_approval.request_generation,
          'requires_confirmation',false,'requires_reason',false,
          'enabled',v_reminder_eligible,'disabled_reason',v_disabled_reason
        ));
        v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
          'code','REQUEST_APPROVAL_AGAIN','label','Request Approval Again',
          'method','POST','path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/renew',
          'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
          'approval_request_id',v_approval.id,
          'approval_request_generation',v_approval.request_generation,
          'requires_confirmation',true,'requires_reason',false,
          'enabled',v_renewal_eligible,
          'disabled_reason',case when v_renewal_eligible then null else 'MANAGER_APPROVAL_REQUEST_NOT_EXPIRED' end
        ));
      end if;
    end if;

    if v_workflow.route='PAPER' and v_workflow.state='AWAITING_PAPER_RETURN' then
      v_paper_pack:=private._candidate_paper_pack_readiness_v1(v_workflow.id,v_workflow.generation);
      v_actions:=v_actions||jsonb_build_array(
        jsonb_build_object(
          'code','DOWNLOAD_PAPER_DOCUMENTS','label','Download Documents','method','GET',
          'path','/candidate-app/v1/timesheets/'||coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)::text||'/paper-pack',
          'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
          'approval_request_id',null,'requires_confirmation',false,'requires_reason',false,
          'enabled',coalesce((v_paper_pack->>'download_available')::boolean,false),
          'disabled_reason',case when coalesce((v_paper_pack->>'download_available')::boolean,false)
            then null else v_paper_pack->>'reason_code' end
        ),
        jsonb_build_object(
          'code','UPLOAD_SIGNED_RETURN','label','Upload Signed Return','method','POST',
          'path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/paper-return',
          'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
          'approval_request_id',null,'requires_confirmation',true,'requires_reason',false,
          'enabled',coalesce((v_paper_pack->>'upload_available')::boolean,false),
          'disabled_reason',case when coalesce((v_paper_pack->>'upload_available')::boolean,false)
            then null else v_paper_pack->>'reason_code' end
        )
      );
    elsif v_workflow.route='PAPER' and v_workflow.state='RECEIVED' then
      v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
        'code','RETRY_FINALISATION','label','Retry Finalisation','method','POST',
        'path','/candidate-app/v1/workflows/'||v_workflow.id::text||'/actions/retry-finalisation',
        'workflow_id',v_workflow.id,'workflow_generation',v_workflow.generation,
        'approval_request_id',null,'requires_confirmation',false,'requires_reason',false,
        'enabled',true,'disabled_reason',null
      ));
    end if;
  end if;

  if coalesce((p_capabilities->>'candidate_no_work_allowed')::boolean,false) then
    v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
      'code','NO_WORK_THIS_WEEK','label','I Did Not Work This Week','method','POST',
      'path','/candidate-app/v1/contract-weeks/'||p_contract_week_id::text||'/no-work',
      'timesheet_id',p_timesheet_id,'contract_week_id',p_contract_week_id,
      'requires_confirmation',true,'requires_reason',false,'enabled',true,'disabled_reason',null
    ));
  end if;
  if coalesce((p_capabilities->>'can_edit_expenses')::boolean,false)
     and coalesce(v_primary->>'code','')<>'ADD_EXPENSES' then
    v_actions:=v_actions||jsonb_build_array(jsonb_build_object(
      'code','ADD_EXPENSES','label','Add Expenses','method','POST',
      'path','/candidate-app/v1/workflows','timesheet_id',p_timesheet_id,
      'contract_week_id',p_contract_week_id,'requires_confirmation',false,
      'requires_reason',false,'enabled',true,'disabled_reason',null
    ));
  end if;
  select coalesce(jsonb_agg(private._candidate_action_invocation_v1(item)
    order by ordinal),'[]'::jsonb)
  into v_actions
  from jsonb_array_elements(v_actions) with ordinality action_item(item,ordinal);
  if v_primary is not null then
    v_primary:=private._candidate_action_invocation_v1(v_primary);
  end if;
  return jsonb_build_object(
    'primary_action',v_primary,
    'available_actions',v_actions,
    'manager_approval',v_manager_approval,
    'paper_pack',v_paper_pack
  );
end;
$function$;

drop function if exists public.candidate_app_timesheet_page_v1(
  uuid,text,text,integer,timestamptz
);

create or replace function public.candidate_app_timesheet_page_v1(
  p_session_id uuid,
  p_environment text,
  p_view text default 'CURRENT',
  p_cursor text default null,
  p_limit integer default 50,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_context jsonb;
  v_candidate_id uuid;
  v_view text:=upper(btrim(coalesce(p_view,'CURRENT')));
  v_snapshot_utc timestamptz:=p_now_utc;
  v_limit integer:=least(greatest(coalesce(p_limit,50),1),100);
  v_cursor_parts text[];
  v_cursor_view text;
  v_cursor_snapshot timestamptz;
  v_cursor_candidate_id uuid;
  v_cursor_date date;
  v_cursor_contract_id uuid;
  v_cursor_additional_seq integer;
  v_cursor_id uuid;
  v_rows jsonb;
  v_next_cursor text;
  v_conflicts jsonb;
begin
  if v_view not in ('CURRENT','HISTORY') then
    raise exception 'CANDIDATE_VIEW_INVALID' using errcode='22023';
  end if;
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_reads');
  v_context:=private._candidate_session_context_v1(p_session_id,p_environment,null,p_now_utc,false);
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;

  if nullif(btrim(coalesce(p_cursor,'')),'') is not null then
    begin
      v_cursor_parts:=string_to_array(p_cursor,'|');
      if cardinality(v_cursor_parts)<>8 then
        raise exception 'CANDIDATE_CURSOR_INVALID' using errcode='22023';
      end if;
      if v_cursor_parts[1]<>'v2' then
        raise exception 'CANDIDATE_CURSOR_VERSION_UNSUPPORTED' using errcode='22023';
      end if;
      v_cursor_view:=upper(v_cursor_parts[2]);
      v_cursor_snapshot:=v_cursor_parts[3]::timestamptz;
      v_cursor_candidate_id:=v_cursor_parts[4]::uuid;
      v_cursor_date:=v_cursor_parts[5]::date;
      v_cursor_contract_id:=v_cursor_parts[6]::uuid;
      v_cursor_additional_seq:=v_cursor_parts[7]::integer;
      v_cursor_id:=v_cursor_parts[8]::uuid;
    exception when others then
      if sqlerrm in (
        'CANDIDATE_CURSOR_VERSION_UNSUPPORTED','CANDIDATE_CURSOR_INVALID'
      ) then raise; end if;
      raise exception 'CANDIDATE_CURSOR_INVALID' using errcode='22023';
    end;
    if v_cursor_view<>v_view then
      raise exception 'CANDIDATE_CURSOR_VIEW_MISMATCH' using errcode='22023';
    end if;
    if v_cursor_candidate_id<>v_candidate_id then
      raise exception 'CANDIDATE_CURSOR_CANDIDATE_MISMATCH' using errcode='22023';
    end if;
    if v_cursor_snapshot is null or v_cursor_snapshot>p_now_utc+interval '1 minute' then
      raise exception 'CANDIDATE_CURSOR_SNAPSHOT_INVALID' using errcode='22023';
    end if;
    if v_cursor_snapshot<p_now_utc-interval '24 hours' then
      raise exception 'CANDIDATE_CURSOR_EXPIRED' using errcode='22023';
    end if;
    v_snapshot_utc:=v_cursor_snapshot;
  end if;

  with candidate_weeks as materialized (
    select cw.*,c.client_id,c.candidate_id,c.weekly_timesheet_source,
           client.name as client_name,
           coalesce(nullif(t.job_title_norm,''),nullif(c.role,'')) as display_job_title,
           coalesce(nullif(t.band,''),nullif(c.band,'')) as display_band,
           coalesce(c.week_ending_weekday_snapshot,effective_client.week_ending_weekday,0) as effective_week_ending_weekday,
           current_window.current_week_ending_date,
           t.booking_id,t.parent_timesheet_id,t.status as timesheet_status,t.submission_mode,t.line_type,t.sheet_scope,t.is_current,
           t.additional_units_week,t.additional_units_per_day,
           tf.additional_units_json,tf.total_hours,tf.processing_status,tf.authorised_at_utc,tf.paid_at_utc,tf.locked_by_invoice_id,
           tf.expenses_pay_ex_vat,tf.expenses_charge_ex_vat,
           tf.mileage_units,tf.mileage_pay_ex_vat,tf.mileage_charge_ex_vat,
           tf.travel_pay_ex_vat,tf.travel_charge_ex_vat,
           tf.accommodation_pay_ex_vat,tf.accommodation_charge_ex_vat,
           tf.other_pay_ex_vat,tf.other_charge_ex_vat,
           private._candidate_record_capabilities_v1(t.timesheet_id,cw.id,'{}'::jsonb) as capabilities
    from public.contract_weeks cw
    join public.contracts c on c.id=cw.contract_id and c.candidate_id=v_candidate_id
    join public.clients client on client.id=c.client_id
    left join lateral (
      select cs.week_ending_weekday
      from public.client_settings cs
      where cs.client_id=c.client_id
        and cs.effective_from<=(v_snapshot_utc at time zone 'Europe/London')::date
      order by cs.effective_from desc,cs.updated_at desc nulls last,cs.id desc
      limit 1
    ) effective_client on true
    cross join lateral (
      select (
        (v_snapshot_utc at time zone 'Europe/London')::date
        +mod(
          coalesce(c.week_ending_weekday_snapshot,effective_client.week_ending_weekday,0)
          -extract(dow from (v_snapshot_utc at time zone 'Europe/London')::date)::integer+7,
          7
        )
      )::date as current_week_ending_date
    ) current_window
    left join public.timesheets t on t.timesheet_id=cw.timesheet_id
    left join lateral (
      select f.* from public.timesheets_financials f
      where f.timesheet_id=t.timesheet_id and f.is_current=true
      order by f.computed_at_utc desc nulls last,f.updated_at desc,f.id desc limit 1
    ) tf on true
    where t.timesheet_id is null or (t.is_current=true and t.archived_at_utc is null)
  ), current_version_resolution as materialized (
    -- Candidate workflow and parent anchors are immutable historical UUIDs.
    -- Resolve every historical member through booking_id to the one current
    -- Candidate-safe row in that version family. A missing or ambiguous
    -- current member deliberately resolves to NULL so the caller fails closed.
    select history.timesheet_id as historical_timesheet_id,
      count(distinct current_week.timesheet_id)::integer as current_count,
      case when count(distinct current_week.timesheet_id)=1
        then min(current_week.timesheet_id::text)::uuid else null::uuid end
        as current_timesheet_id
    from public.timesheets history
    join public.timesheets current_row
      on nullif(btrim(coalesce(current_row.booking_id,'')),'')
        =nullif(btrim(coalesce(history.booking_id,'')),'')
      and current_row.contract_id is not distinct from history.contract_id
      and current_row.week_ending_date is not distinct from history.week_ending_date
      and current_row.is_current=true
      and current_row.archived_at_utc is null
    join candidate_weeks current_week
      on current_week.timesheet_id=current_row.timesheet_id
    where nullif(btrim(coalesce(history.booking_id,'')),'') is not null
    group by history.timesheet_id
  ), expense_carriers as materialized (
    select expense_row.*,
      abs(coalesce(expense_row.expenses_pay_ex_vat,0))+abs(coalesce(expense_row.expenses_charge_ex_vat,0))+
      abs(coalesce(expense_row.mileage_units,0))+abs(coalesce(expense_row.mileage_pay_ex_vat,0))+
      abs(coalesce(expense_row.mileage_charge_ex_vat,0))+
      abs(coalesce(expense_row.travel_pay_ex_vat,0))+abs(coalesce(expense_row.travel_charge_ex_vat,0))+
      abs(coalesce(expense_row.accommodation_pay_ex_vat,0))+abs(coalesce(expense_row.accommodation_charge_ex_vat,0))+
      abs(coalesce(expense_row.other_pay_ex_vat,0))+abs(coalesce(expense_row.other_charge_ex_vat,0)) as expense_value
    from candidate_weeks expense_row
    where expense_row.capabilities->>'record_role'='EXPENSE_ONLY'
       or upper(coalesce(expense_row.line_type::text,'')) in ('EXPENSES','MILEAGE')
  ), expense_carrier_resolution as materialized (
    select carrier.id as carrier_contract_week_id,carrier.timesheet_id as carrier_timesheet_id,
      carrier.contract_id,carrier.week_ending_date,
      case when workflow_anchor.workflow_count>1 then null::uuid
        when workflow_anchor.workflow_count=1 and workflow_anchor.timesheet_id is null then null::uuid
        when workflow_anchor.timesheet_id is not null then workflow_anchor.timesheet_id
        when carrier.parent_timesheet_id is not null and parent_anchor.timesheet_id is null then null::uuid
        when parent_anchor.timesheet_id is not null then parent_anchor.timesheet_id
        when base_anchor.anchor_count>1 then null::uuid
        when base_anchor.timesheet_id is not null then base_anchor.timesheet_id
        when additional_anchor.anchor_count>1 then null::uuid
        else coalesce(parent_anchor.timesheet_id,additional_anchor.timesheet_id) end as display_timesheet_id,
      case
        when workflow_anchor.workflow_count>1 then 'AMBIGUOUS_WORKFLOW_ANCHOR'
        when workflow_anchor.workflow_count=1 and workflow_anchor.timesheet_id is null then 'INVALID_WORKFLOW_ANCHOR'
        when carrier.parent_timesheet_id is not null and parent_anchor.timesheet_id is null then 'INVALID_PARENT_ANCHOR'
        when base_anchor.anchor_count>1 then 'EXPENSE_DISPLAY_ANCHOR_AMBIGUOUS'
        when base_anchor.timesheet_id is null and additional_anchor.anchor_count>1 then 'EXPENSE_DISPLAY_ANCHOR_AMBIGUOUS'
        when coalesce(workflow_anchor.timesheet_id,parent_anchor.timesheet_id,base_anchor.timesheet_id,additional_anchor.timesheet_id) is null
          and carrier.expense_value<>0 then 'EXPENSE_DISPLAY_ANCHOR_NOT_FOUND'
        else null end as conflict_code,
      carrier.expenses_pay_ex_vat,carrier.mileage_pay_ex_vat,carrier.travel_pay_ex_vat,
      carrier.accommodation_pay_ex_vat,carrier.other_pay_ex_vat,carrier.expense_value
    from expense_carriers carrier
    left join lateral (
      select count(distinct workflow.id)::integer as workflow_count,
        min(anchor_row.timesheet_id::text)::uuid as timesheet_id
      from public.candidate_submission_workflows workflow
      left join current_version_resolution workflow_anchor_family
        on workflow_anchor_family.historical_timesheet_id=workflow.anchor_timesheet_id
      left join candidate_weeks anchor_row
        on anchor_row.timesheet_id=coalesce(
          workflow_anchor_family.current_timesheet_id,workflow.anchor_timesheet_id
        )
        and anchor_row.contract_id=carrier.contract_id
        and anchor_row.week_ending_date=carrier.week_ending_date
        and anchor_row.capabilities->>'record_role'<>'EXPENSE_ONLY'
      where workflow.candidate_id=v_candidate_id
        and workflow.contract_id=carrier.contract_id
        and workflow.week_ending_date=carrier.week_ending_date
        and (workflow.target_timesheet_id=carrier.timesheet_id or workflow.contract_week_id=carrier.id)
        and workflow.state not in ('CANCELLED','SUPERSEDED','REJECTED')
    ) workflow_anchor on true
    left join lateral (
      select parent_row.timesheet_id
      from candidate_weeks parent_row
      left join current_version_resolution parent_family
        on parent_family.historical_timesheet_id=carrier.parent_timesheet_id
      where parent_row.timesheet_id=coalesce(
          parent_family.current_timesheet_id,carrier.parent_timesheet_id
        )
        and parent_row.contract_id=carrier.contract_id
        and parent_row.week_ending_date=carrier.week_ending_date
        and parent_row.capabilities->>'record_role'<>'EXPENSE_ONLY'
      limit 1
    ) parent_anchor on true
    left join lateral (
      select count(*)::integer as anchor_count,
        min(hours_row.timesheet_id::text)::uuid as timesheet_id
      from candidate_weeks hours_row
      where hours_row.contract_id=carrier.contract_id
        and hours_row.week_ending_date=carrier.week_ending_date
        and hours_row.additional_seq=0
        and (
          coalesce(hours_row.total_hours,0)>0
          or private._candidate_json_numeric_sum(coalesce(hours_row.additional_units_json,'{}'::jsonb))>0
          or private._candidate_json_numeric_sum(coalesce(hours_row.additional_units_week,'{}'::jsonb))
            +private._candidate_json_numeric_sum(coalesce(hours_row.additional_units_per_day,'{}'::jsonb))>0
        )
        and hours_row.capabilities->>'record_role'<>'EXPENSE_ONLY'
    ) base_anchor on true
    left join lateral (
      select count(*)::integer as anchor_count,
        min(hours_row.timesheet_id::text)::uuid as timesheet_id
      from candidate_weeks hours_row
      where hours_row.contract_id=carrier.contract_id
        and hours_row.week_ending_date=carrier.week_ending_date
        and hours_row.additional_seq>0
        and (
          coalesce(hours_row.total_hours,0)>0
          or private._candidate_json_numeric_sum(coalesce(hours_row.additional_units_json,'{}'::jsonb))>0
          or private._candidate_json_numeric_sum(coalesce(hours_row.additional_units_week,'{}'::jsonb))
            +private._candidate_json_numeric_sum(coalesce(hours_row.additional_units_per_day,'{}'::jsonb))>0
        )
        and hours_row.capabilities->>'record_role'<>'EXPENSE_ONLY'
    ) additional_anchor on true
  ), expense_anchor_totals as materialized (
    select display_timesheet_id,
      sum(expenses_pay_ex_vat) expenses_pay_ex_vat,
      sum(mileage_pay_ex_vat) mileage_pay_ex_vat,
      sum(travel_pay_ex_vat) travel_pay_ex_vat,
      sum(accommodation_pay_ex_vat) accommodation_pay_ex_vat,
      sum(other_pay_ex_vat) other_pay_ex_vat
    from expense_carrier_resolution
    where display_timesheet_id is not null and conflict_code is null
    group by display_timesheet_id
  ), workflow_overlay as materialized (
    select resolved.display_timesheet_id,
      jsonb_agg(jsonb_build_object(
        'workflow_id',resolved.id,'workflow_kind',resolved.workflow_kind,'state',resolved.state,
        'claim_family',resolved.claim_family,
        'target_timesheet_id',resolved.target_timesheet_id,'anchor_timesheet_id',resolved.anchor_timesheet_id,
        'rejection_reason',resolved.rejection_reason,'rejection_scope',resolved.rejection_scope,
        'required_resubmission_action',case
          when resolved.state<>'REJECTED' or not resolved.rejection_actionable then null
          when resolved.workflow_kind='CONTRACT_EXPENSE'
            or resolved.rejection_scope='COMPLETE_EXPENSE_CLAIM'
            then 'RESUBMIT_EXPENSE_CLAIM'
          when resolved.workflow_kind='CONTRACT_COMBINED'
            then 'RESUBMIT_TIMESHEET_AND_EXPENSES'
          else 'RESUBMIT_TIMESHEET' end,
        'rejection_actionable',resolved.rejection_actionable,
        'updated_at_utc',resolved.updated_at_utc
      ) order by resolved.updated_at_utc desc,resolved.id) as workflows
    from (
      select classified.*,
        case
          -- Rejection rotates the submitted timesheet to a replacement current
          -- version while the immutable workflow continues to reference the
          -- historical submitted target. Resolve rejected workflows through the
          -- current contract-week authority so the Candidate card retains the
          -- rejection reason, scope and server-owned recovery action.
          when classified.state='REJECTED'
            and classified.claim_family='EXPENSES' then (
            select resolution.display_timesheet_id
            from expense_carrier_resolution resolution
            where resolution.carrier_contract_week_id=classified.contract_week_id
              and resolution.conflict_code is null
            limit 1
          )
          when classified.state='REJECTED' then (
            select current_week.timesheet_id
            from candidate_weeks current_week
            where current_week.id=classified.contract_week_id
            limit 1
          )
          when classified.claim_family='EXPENSES' then coalesce(
          (select resolution.display_timesheet_id from expense_carrier_resolution resolution
            where resolution.carrier_timesheet_id=classified.target_timesheet_id limit 1),
          (select family.current_timesheet_id from current_version_resolution family
            where family.historical_timesheet_id=classified.anchor_timesheet_id
              and family.current_count=1),
          (select direct_anchor.timesheet_id from candidate_weeks direct_anchor
            where direct_anchor.timesheet_id=classified.anchor_timesheet_id limit 1)
          )
          else coalesce(
            (select family.current_timesheet_id from current_version_resolution family
              where family.historical_timesheet_id=coalesce(
                classified.target_timesheet_id,classified.anchor_timesheet_id
              ) and family.current_count=1),
            (select direct_target.timesheet_id from candidate_weeks direct_target
              where direct_target.timesheet_id=coalesce(
                classified.target_timesheet_id,classified.anchor_timesheet_id
              ) limit 1)
          )
        end as display_timesheet_id
      from (
        select w.*,
          case when w.workflow_kind='CONTRACT_EXPENSE'
              or w.rejection_scope='COMPLETE_EXPENSE_CLAIM'
            then 'EXPENSES' else 'HOURS' end as claim_family,
          case when w.state<>'REJECTED' then false
            else not private._candidate_rejection_replaced_v1(w.id)
          end as rejection_actionable
        from public.candidate_submission_workflows w
        where w.candidate_id=v_candidate_id and w.state<>'SUPERSEDED'
      ) classified
    ) resolved
    where resolved.display_timesheet_id is not null
    group by resolved.display_timesheet_id
  ), visible as materialized (
    select base.*,
      coalesce(totals.expenses_pay_ex_vat,0) as overlay_expenses_pay_ex_vat,
      coalesce(totals.mileage_pay_ex_vat,0) as overlay_mileage_pay_ex_vat,
      coalesce(totals.travel_pay_ex_vat,0) as overlay_travel_pay_ex_vat,
      coalesce(totals.accommodation_pay_ex_vat,0) as overlay_accommodation_pay_ex_vat,
      coalesce(totals.other_pay_ex_vat,0) as overlay_other_pay_ex_vat,
      coalesce(workflows.workflows,'[]'::jsonb) as workflows,
      null::text as expense_overlay_conflict_code,
      membership.tab_bucket
    from candidate_weeks base
    left join expense_anchor_totals totals on totals.display_timesheet_id=base.timesheet_id
    left join workflow_overlay workflows on workflows.display_timesheet_id=base.timesheet_id
    cross join lateral (
      select case
        when base.paid_at_utc is null or base.paid_at_utc>v_snapshot_utc then 'CURRENT'
        when base.paid_at_utc>=v_snapshot_utc-interval '7 days' then 'CURRENT'
        when base.paid_at_utc<v_snapshot_utc-interval '7 days'
          and base.week_ending_date between base.current_week_ending_date-105
            and base.current_week_ending_date then 'HISTORY'
        else 'EXCLUDED' end as tab_bucket
    ) membership
    where not exists(select 1 from expense_carriers carrier where carrier.id=base.id)
      and base.week_ending_date<=base.current_week_ending_date
      and membership.tab_bucket=v_view
      and (
        v_cursor_date is null
        or (
          base.week_ending_date,
          base.contract_id,
          base.additional_seq,
          base.id
        )<(v_cursor_date,v_cursor_contract_id,v_cursor_additional_seq,v_cursor_id)
      )
  ), page as materialized (
    select * from visible
    order by week_ending_date desc,contract_id desc,additional_seq desc,id desc
    limit v_limit+1
  ), delivered as materialized (
    select page.*,
      (
        select workflow_item->>'state'
        from jsonb_array_elements(page.workflows) workflow_item
        where workflow_item->>'state' in (
          'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
          'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
          'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
          'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
          'AWAITING_PAPER_RETURN','RECEIVED','REFUSED'
        )
        limit 1
      ) as active_workflow_state,
      (
        select workflow_item
        from jsonb_array_elements(page.workflows) workflow_item
        where workflow_item->>'state'='REJECTED'
          and coalesce((workflow_item->>'rejection_actionable')::boolean,false)
        limit 1
      ) as rejected_workflow,
      (
        select coalesce(jsonb_agg(workflow_item order by
          workflow_item->>'updated_at_utc' desc,workflow_item->>'workflow_id'),'[]'::jsonb)
        from jsonb_array_elements(page.workflows) workflow_item
        where workflow_item->>'state'='REJECTED'
          and coalesce((workflow_item->>'rejection_actionable')::boolean,false)
      ) as actionable_rejections
    from page
    order by week_ending_date desc,contract_id desc,additional_seq desc,id desc
    limit v_limit
  )
  select
    coalesce(jsonb_agg(jsonb_build_object(
      'contract_week_id',d.id,
      'contract_id',d.contract_id,
      'timesheet_id',d.timesheet_id,
      'client_name',d.client_name,
      'job_title',d.display_job_title,
      'band',d.display_band,
      'week_ending_date',d.week_ending_date,
      'week_ending_label',private._candidate_week_ending_label_v1(d.week_ending_date),
      'week_ending_weekday',btrim(to_char(d.week_ending_date,'FMDay')),
      'additional_seq',d.additional_seq,
      'tab_bucket',d.tab_bucket,
      'effective_current_week_ending_date',d.current_week_ending_date,
      'paid_at_utc',case when d.paid_at_utc<=v_snapshot_utc then d.paid_at_utc else null end,
      'contract_week_status',d.status,
      'timesheet_status',d.timesheet_status,
      'processing_status',d.processing_status,
      'paid',d.paid_at_utc is not null and d.paid_at_utc<=v_snapshot_utc,
      'authorised',d.authorised_at_utc is not null,
      'total_hours',coalesce(d.total_hours,0),
      'expenses',jsonb_build_object(
        'expenses_pay_ex_vat',coalesce(d.overlay_expenses_pay_ex_vat,0),
        'mileage_pay_ex_vat',coalesce(d.overlay_mileage_pay_ex_vat,0),
        'travel_pay_ex_vat',coalesce(d.overlay_travel_pay_ex_vat,0),
        'accommodation_pay_ex_vat',coalesce(d.overlay_accommodation_pay_ex_vat,0),
        'other_pay_ex_vat',coalesce(d.overlay_other_pay_ex_vat,0)
      ),
      'expense_overlay_conflict_code',d.expense_overlay_conflict_code,
      'workflows',d.workflows,
      'rejections',d.actionable_rejections,
      'record_role',d.capabilities->'record_role',
      'route_family',d.capabilities->'route_family',
      'candidate_status_code',private._candidate_status_code_v1(
        d.paid_at_utc is not null and d.paid_at_utc<=v_snapshot_utc,
        d.authorised_at_utc is not null,
        d.locked_by_invoice_id is not null
          or upper(coalesce(d.timesheet_status::text,''))='INVOICED',
        d.active_workflow_state,d.rejected_workflow is not null,
        d.processing_status::text,d.status::text
      ),
      'payment_state',case when d.paid_at_utc is not null and d.paid_at_utc<=v_snapshot_utc
        then 'PAID' else 'UNPAID' end,
      'invoice_state',case
        when d.paid_at_utc is not null and d.paid_at_utc<=v_snapshot_utc then 'PAID'
        when d.locked_by_invoice_id is not null or upper(coalesce(d.timesheet_status::text,''))='INVOICED'
          then 'INVOICED_NOT_PAID'
        else 'NOT_INVOICED' end,
      'manager_approval_state',(
        select workflow_item->>'state'
        from jsonb_array_elements(d.workflows) workflow_item
        where workflow_item->>'state' in (
          'READY_FOR_MANAGER_APPROVAL','AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
          'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE','FINALISED','REFUSED'
        )
        limit 1
      ),
      'rejection_reason',case
        when (d.paid_at_utc is not null and d.paid_at_utc<=v_snapshot_utc) or d.authorised_at_utc is not null
          or d.locked_by_invoice_id is not null
          or upper(coalesce(d.timesheet_status::text,''))='INVOICED' then null
        else nullif(d.rejected_workflow->>'rejection_reason','') end,
      'rejection_scope',case
        when (d.paid_at_utc is not null and d.paid_at_utc<=v_snapshot_utc) or d.authorised_at_utc is not null
          or d.locked_by_invoice_id is not null
          or upper(coalesce(d.timesheet_status::text,''))='INVOICED'
          or d.rejected_workflow is null then d.capabilities->'reject_scope'
        else d.rejected_workflow->'rejection_scope' end,
      'rejection',case
        when (d.paid_at_utc is not null and d.paid_at_utc<=v_snapshot_utc) or d.authorised_at_utc is not null
          or d.locked_by_invoice_id is not null
          or upper(coalesce(d.timesheet_status::text,''))='INVOICED'
          or d.rejected_workflow is null then null
        else jsonb_build_object(
          'workflow_id',d.rejected_workflow->'workflow_id',
          'reason',d.rejected_workflow->'rejection_reason',
          'scope',d.rejected_workflow->'rejection_scope',
          'required_action',d.rejected_workflow->'required_resubmission_action'
        ) end,
      'primary_action',private._candidate_action_invocation_v1(private._candidate_timesheet_primary_action_v1(
        private._candidate_status_code_v1(
          d.paid_at_utc is not null and d.paid_at_utc<=v_snapshot_utc,
          d.authorised_at_utc is not null,
          d.locked_by_invoice_id is not null
            or upper(coalesce(d.timesheet_status::text,''))='INVOICED',
          d.active_workflow_state,d.rejected_workflow is not null,
          d.processing_status::text,d.status::text
        ),
        d.workflows,d.capabilities,d.timesheet_id,d.id
      )),
      'detail_target',case
        when d.rejected_workflow is not null then jsonb_build_object(
          'identity_kind','WORKFLOW','id',d.rejected_workflow->>'workflow_id',
          'path','/candidate-app/v1/workflows/'||(d.rejected_workflow->>'workflow_id')||'/timesheet-detail'
        )
        when d.timesheet_id is not null then jsonb_build_object(
          'identity_kind','TIMESHEET','id',d.timesheet_id,
          'path','/candidate-app/v1/timesheets/'||d.timesheet_id::text
        )
        else jsonb_build_object(
          'identity_kind','CONTRACT_WEEK','id',d.id,
          'path','/candidate-app/v1/contract-weeks/'||d.id::text||'/detail'
        ) end,
      'actions',jsonb_build_object(
        'can_edit_hours',d.capabilities->'can_edit_hours',
        'can_edit_expenses',d.capabilities->'can_edit_expenses',
        'candidate_paper_submission_allowed',d.capabilities->'candidate_paper_submission_allowed',
        'candidate_no_work_allowed',d.capabilities->'candidate_no_work_allowed',
        'can_reject_candidate_submission',d.capabilities->'can_reject_candidate_submission',
        'reject_scope',d.capabilities->'reject_scope'
      )
    ) order by d.week_ending_date desc,d.contract_id desc,d.additional_seq desc,d.id desc),'[]'::jsonb),
    case when (select count(*) from page)>v_limit then
      (select 'v2|'||v_view||'|'||to_char(v_snapshot_utc at time zone 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')||'|'||v_candidate_id::text||'|'
          ||p.week_ending_date::text||'|'||p.contract_id::text||'|'
          ||p.additional_seq::text||'|'||p.id::text
       from delivered p
       order by p.week_ending_date asc,p.contract_id asc,p.additional_seq asc,p.id asc limit 1)
    else null end,
    (
      select coalesce(jsonb_agg(jsonb_build_object(
        'contract_id',conflict.contract_id,
        'week_ending_date',conflict.week_ending_date,
        'code',conflict.conflict_code
      ) order by conflict.week_ending_date desc,conflict.contract_id),'[]'::jsonb)
      from expense_carrier_resolution conflict
      where conflict.conflict_code is not null
    )
  into v_rows,v_next_cursor,v_conflicts
  from delivered d;

  return jsonb_build_object(
    'ok',true,
    'view',v_view,
    'default_view','CURRENT',
    'snapshot_utc',v_snapshot_utc,
    'paid_current_cutoff_utc',v_snapshot_utc-interval '7 days',
    'items',v_rows,
    'next_cursor',v_next_cursor,
    'cursor_version','v2',
    'readiness_conflicts',coalesce(v_conflicts,'[]'::jsonb),
    'limit',v_limit
  );
end;
$function$;

create or replace function public.candidate_app_timesheet_detail_v1(
  p_session_id uuid,
  p_environment text,
  p_timesheet_id uuid default null,
  p_contract_week_id uuid default null,
  p_workflow_id uuid default null,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_context jsonb;
  v_candidate_id uuid;
  v_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_client public.clients%rowtype;
  v_timesheet public.timesheets%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_capabilities jsonb;
  v_evidence jsonb;
  v_components jsonb;
  v_claims jsonb;
  v_document_state jsonb:='{}'::jsonb;
  v_effective_week_ending_weekday integer;
  v_effective_current_week_ending_date date;
  v_tab_bucket text;
  v_candidate_status_code text;
  v_active_workflow_state text;
  v_rejected_workflow jsonb;
  v_primary_action jsonb;
  v_action_contract jsonb;
  v_detail_source_timesheet_id uuid;
begin
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_reads');
  if num_nonnulls(p_timesheet_id,p_contract_week_id,p_workflow_id)<>1 then
    raise exception 'CANDIDATE_DETAIL_IDENTITY_INVALID' using errcode='22023';
  end if;
  v_context:=private._candidate_session_context_v1(p_session_id,p_environment,null,p_now_utc,false);
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;

  if p_workflow_id is not null then
    select * into v_workflow from public.candidate_submission_workflows where id=p_workflow_id and candidate_id=v_candidate_id;
    if not found then raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002'; end if;
    v_detail_source_timesheet_id:=case
      when v_workflow.workflow_kind='CONTRACT_EXPENSE'
        or v_workflow.rejection_scope='COMPLETE_EXPENSE_CLAIM'
        then coalesce(v_workflow.anchor_timesheet_id,v_workflow.target_timesheet_id)
      else coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id) end;
    if v_detail_source_timesheet_id is not null then
      select current_version.timesheet_id into p_timesheet_id
      from public.timesheets source_version
      join public.timesheets current_version on current_version.is_current=true
        and current_version.archived_at_utc is null
        and (
          (nullif(btrim(coalesce(source_version.booking_id,'')),'') is not null
            and current_version.booking_id=source_version.booking_id
            and current_version.contract_id is not distinct from source_version.contract_id
            and current_version.week_ending_date is not distinct from source_version.week_ending_date)
          or (nullif(btrim(coalesce(source_version.booking_id,'')),'') is null
            and current_version.timesheet_id=source_version.timesheet_id)
        )
      where source_version.timesheet_id=v_detail_source_timesheet_id
        and upper(coalesce(current_version.line_type::text,'')) not in ('EXPENSES','MILEAGE')
      order by current_version.version desc,current_version.timesheet_id
      limit 1;
    end if;
    if p_timesheet_id is not null then
      select week_row.id into p_contract_week_id
      from public.contract_weeks week_row
      where week_row.timesheet_id=p_timesheet_id
        and week_row.contract_id=v_workflow.contract_id
        and week_row.week_ending_date=v_workflow.week_ending_date
      order by week_row.updated_at desc,week_row.id desc limit 1;
    end if;
    if p_contract_week_id is null then p_contract_week_id:=v_workflow.contract_week_id; end if;
  end if;
  if p_contract_week_id is not null then
    select * into v_week from public.contract_weeks where id=p_contract_week_id;
  else
    select * into v_week from public.contract_weeks where timesheet_id=p_timesheet_id order by updated_at desc,id desc limit 1;
  end if;
  if not found then raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_week.contract_id and candidate_id=v_candidate_id;
  if not found then raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_client from public.clients where id=v_contract.client_id;
  select coalesce(
    v_contract.week_ending_weekday_snapshot,
    (
      select settings.week_ending_weekday
      from public.client_settings settings
      where settings.client_id=v_contract.client_id
        and settings.effective_from<=(p_now_utc at time zone 'Europe/London')::date
      order by settings.effective_from desc,settings.updated_at desc nulls last,settings.id desc
      limit 1
    ),0
  ) into v_effective_week_ending_weekday;
  v_effective_current_week_ending_date:=(
    (p_now_utc at time zone 'Europe/London')::date
    +mod(
      v_effective_week_ending_weekday
      -extract(dow from (p_now_utc at time zone 'Europe/London')::date)::integer+7,7
    )
  )::date;
  if p_timesheet_id is null then p_timesheet_id:=v_week.timesheet_id; end if;
  if p_timesheet_id is not null then
    select * into v_timesheet from public.timesheets where timesheet_id=p_timesheet_id and is_current=true and archived_at_utc is null;
    if not found then raise exception 'CANDIDATE_DETAIL_NOT_FOUND' using errcode='P0002'; end if;
    select * into v_fin from public.timesheets_financials where timesheet_id=p_timesheet_id and is_current=true
    order by computed_at_utc desc nulls last,updated_at desc,id desc limit 1;
  end if;
  v_capabilities:=private._candidate_record_capabilities_v1(p_timesheet_id,v_week.id,'{}'::jsonb);

  select coalesce(jsonb_agg(jsonb_build_object(
    'id',e.id,'kind',e.kind,'document_role',e.document_role,'display_name',e.display_name,
    'processing_state',e.processing_state,'created_at',e.created_at
  ) order by e.created_at,e.id),'[]'::jsonb)
  into v_evidence from public.timesheet_evidence e
  where e.timesheet_id=p_timesheet_id and e.processing_state<>'SUPERSEDED';

  select coalesce(jsonb_agg(jsonb_build_object(
    'id',c.id,'workflow_id',c.workflow_id,'workflow_generation',c.workflow_generation,
    'component_kind',c.component_kind,
    'expense_category',c.expense_category,'document_role',c.document_role,'state',c.state,
    'media_type',c.media_type,'byte_size',c.byte_size,'created_at_utc',c.created_at_utc,
    'required',c.required,'review_ordinal',c.review_ordinal,
    'review_document_ready',c.review_render_state='READY',
    'review_document_generation',c.workflow_generation,
    'review_page_count',c.review_page_count,
    'review_content_sha256',case when c.review_content_sha256 is null then null else encode(c.review_content_sha256,'hex') end,
    'final_signed_document_ready',c.final_signed_render_state='READY',
    'final_signed_page_count',c.final_signed_page_count
  ) order by c.created_at_utc,c.component_no),'[]'::jsonb)
  into v_components from public.candidate_submission_components c
  join public.candidate_submission_workflows w on w.id=c.workflow_id
  where w.candidate_id=v_candidate_id
    and private._candidate_workflow_maps_to_card_v1(w.id,p_timesheet_id,v_week.id)
    and c.state<>'SUPERSEDED';

  select coalesce(jsonb_agg(jsonb_build_object(
    'workflow_id',w.id,'workflow_kind',w.workflow_kind,'state',w.state,'generation',w.generation,
    'detail_action_owner',w.id=p_workflow_id,
    'claim_family',rejection_policy.claim_family,
    'route',w.route,'target_timesheet_id',w.target_timesheet_id,
    'anchor_timesheet_id',w.anchor_timesheet_id,'issue_codes',w.issue_codes,
    'rejection_reason',w.rejection_reason,'rejection_scope',w.rejection_scope,
    'required_resubmission_action',case
      when w.state<>'REJECTED' or not rejection_policy.rejection_actionable then null
      when w.workflow_kind='CONTRACT_EXPENSE' or w.rejection_scope='COMPLETE_EXPENSE_CLAIM'
        then 'RESUBMIT_EXPENSE_CLAIM'
      when w.workflow_kind='CONTRACT_COMBINED' then 'RESUBMIT_TIMESHEET_AND_EXPENSES'
      else 'RESUBMIT_TIMESHEET' end,
    'rejection_actionable',rejection_policy.rejection_actionable,
    'review_document_ready',exists(select 1 from public.candidate_submission_components review_component
      where review_component.workflow_id=w.id and review_component.workflow_generation=w.generation
        and review_component.required=true and review_component.state<>'SUPERSEDED')
      and not exists(select 1 from public.candidate_submission_components review_component
      where review_component.workflow_id=w.id and review_component.workflow_generation=w.generation
        and review_component.required=true and review_component.state<>'SUPERSEDED'
        and review_component.review_render_state<>'READY'),
    'review_document_generation',w.generation,
    'review_page_count',(select sum(coalesce(review_component.review_page_count,0))
      from public.candidate_submission_components review_component
      where review_component.workflow_id=w.id and review_component.workflow_generation=w.generation
        and review_component.required=true and review_component.state<>'SUPERSEDED'),
    'manager_approval_state',coalesce((select approval.state
      from public.candidate_approval_requests approval where approval.workflow_id=w.id
        and approval.workflow_generation=w.generation
      order by approval.request_generation desc,approval.created_at_utc desc limit 1),w.state),
    'final_signed_document_ready',exists(select 1 from public.candidate_submission_components final_component
      where final_component.workflow_id=w.id and final_component.workflow_generation=w.generation
        and final_component.required=true and final_component.state<>'SUPERSEDED')
      and not exists(select 1 from public.candidate_submission_components final_component
      where final_component.workflow_id=w.id and final_component.workflow_generation=w.generation
        and final_component.required=true and final_component.state<>'SUPERSEDED'
        and final_component.final_signed_render_state<>'READY'),
    'updated_at_utc',w.updated_at_utc
  ) order by w.updated_at_utc desc,w.id desc),'[]'::jsonb)
  into v_claims from public.candidate_submission_workflows w
  cross join lateral (
    select
      case when w.workflow_kind='CONTRACT_EXPENSE'
          or w.rejection_scope='COMPLETE_EXPENSE_CLAIM'
        then 'EXPENSES' else 'HOURS' end as claim_family,
      case when w.state<>'REJECTED' then false
        else not private._candidate_rejection_replaced_v1(w.id)
      end as rejection_actionable
  ) rejection_policy
  where w.candidate_id=v_candidate_id and w.contract_id=v_contract.id
    and w.week_ending_date=v_week.week_ending_date and w.state<>'SUPERSEDED'
    and private._candidate_workflow_maps_to_card_v1(w.id,p_timesheet_id,v_week.id);

  select jsonb_build_object(
    'workflow_id',document_workflow.id,
    'workflow_generation',document_workflow.generation,
    'review_document_ready',coalesce(document_readiness.review_ready,false),
    'review_document_component_id',hours_component.id,
    'review_document_generation',hours_component.workflow_generation,
    'review_page_count',hours_component.review_page_count,
    'manager_approval_state',coalesce(latest_approval.state,document_workflow.state),
    'final_signed_document_ready',coalesce(document_readiness.final_ready,false)
  ) into v_document_state
  from public.candidate_submission_workflows document_workflow
  left join lateral (
    select component.* from public.candidate_submission_components component
    where component.workflow_id=document_workflow.id
      and component.workflow_generation=document_workflow.generation
      and component.component_kind='HOURS_TIMESHEET' and component.state<>'SUPERSEDED'
    order by component.review_ordinal,component.id limit 1
  ) hours_component on true
  left join lateral (
    select
      count(*)>0 and bool_and(component.review_render_state='READY') as review_ready,
      count(*)>0 and bool_and(component.final_signed_render_state='READY') as final_ready
    from public.candidate_submission_components component
    where component.workflow_id=document_workflow.id
      and component.workflow_generation=document_workflow.generation
      and component.required=true and component.state<>'SUPERSEDED'
  ) document_readiness on true
  left join lateral (
    select approval.* from public.candidate_approval_requests approval
    where approval.workflow_id=document_workflow.id
      and approval.workflow_generation=document_workflow.generation
    order by approval.request_generation desc,approval.created_at_utc desc limit 1
  ) latest_approval on true
  where document_workflow.candidate_id=v_candidate_id
    and private._candidate_workflow_maps_to_card_v1(
      document_workflow.id,p_timesheet_id,v_week.id
    )
    and document_workflow.state<>'SUPERSEDED'
  order by (document_workflow.id=p_workflow_id) desc,document_workflow.updated_at_utc desc
  limit 1;

  select workflow_item->>'state' into v_active_workflow_state
  from jsonb_array_elements(coalesce(v_claims,'[]'::jsonb)) workflow_item
  where workflow_item->>'state' in (
    'CREATED','WORKER_DRAFT','WORKER_SUBMITTED',
    'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT','READY_FOR_MANAGER_APPROVAL',
    'AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED',
    'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT','READY_TO_FINALISE',
    'AWAITING_PAPER_RETURN','RECEIVED','REFUSED'
  )
  order by coalesce((workflow_item->>'detail_action_owner')::boolean,false) desc,
    workflow_item->>'updated_at_utc' desc,workflow_item->>'workflow_id'
  limit 1;
  select workflow_item into v_rejected_workflow
  from jsonb_array_elements(coalesce(v_claims,'[]'::jsonb)) workflow_item
  where workflow_item->>'state'='REJECTED'
    and coalesce((workflow_item->>'rejection_actionable')::boolean,false)
  order by coalesce((workflow_item->>'detail_action_owner')::boolean,false) desc,
    workflow_item->>'updated_at_utc' desc,workflow_item->>'workflow_id'
  limit 1;
  v_candidate_status_code:=private._candidate_status_code_v1(
    v_fin.paid_at_utc is not null,
    v_fin.authorised_at_utc is not null,
    v_fin.locked_by_invoice_id is not null
      or upper(coalesce(v_timesheet.status::text,''))='INVOICED',
    v_active_workflow_state,v_rejected_workflow is not null,
    v_fin.processing_status::text,v_week.status::text
  );
  v_tab_bucket:=case
    when v_week.week_ending_date>v_effective_current_week_ending_date then 'EXCLUDED'
    when v_fin.paid_at_utc is null or v_fin.paid_at_utc>p_now_utc then 'CURRENT'
    when v_fin.paid_at_utc>=p_now_utc-interval '7 days' then 'CURRENT'
    when v_fin.paid_at_utc<p_now_utc-interval '7 days'
      and v_week.week_ending_date between v_effective_current_week_ending_date-105
        and v_effective_current_week_ending_date then 'HISTORY'
    else 'EXCLUDED' end;
  v_primary_action:=private._candidate_timesheet_primary_action_v1(
    v_candidate_status_code,v_claims,v_capabilities,p_timesheet_id,v_week.id
  );
  v_action_contract:=private._candidate_timesheet_action_contract_v1(
    v_candidate_status_code,v_claims,v_capabilities,p_timesheet_id,v_week.id,p_now_utc
  );
  v_primary_action:=v_action_contract->'primary_action';
  if v_active_workflow_state='AWAITING_PAPER_RETURN' then
    v_candidate_status_code:=case v_action_contract->'paper_pack'->>'state'
      when 'READY' then 'AWAITING_SIGNED_DOCUMENTS'
      when 'FAILED' then 'DOCUMENT_PREPARATION_FAILED'
      when 'RETIRED' then 'PAPER_DELIVERY_RETIRED'
      when 'STALE' then 'PAPER_DELIVERY_STALE'
      else 'PREPARING_DOCUMENTS' end;
  end if;

  return jsonb_build_object(
    'ok',true,
    'week_ending_label',private._candidate_week_ending_label_v1(v_week.week_ending_date),
    'candidate_status_code',v_candidate_status_code,
    'list_membership',jsonb_build_object(
      'tab_bucket',v_tab_bucket,
      'effective_current_week_ending_date',v_effective_current_week_ending_date,
      'paid_current_cutoff_utc',p_now_utc-interval '7 days'
    ),
    'primary_action',v_primary_action,
    'available_actions',coalesce(v_action_contract->'available_actions','[]'::jsonb),
    'manager_approval',v_action_contract->'manager_approval',
    'paper_pack',v_action_contract->'paper_pack',
    'contract_week',jsonb_build_object(
      'id',v_week.id,'contract_id',v_contract.id,'week_ending_date',v_week.week_ending_date,
      'week_ending_weekday',btrim(to_char(v_week.week_ending_date,'FMDay')),
      'client_name',v_client.name,'job_title',v_contract.role,'band',v_contract.band,
      'additional_seq',v_week.additional_seq,'status',v_week.status,'planned_schedule_json',v_week.planned_schedule_json
    ),
    'timesheet',case when v_timesheet.timesheet_id is null then null else jsonb_build_object(
      'id',v_timesheet.timesheet_id,'status',v_timesheet.status,'submission_mode',v_timesheet.submission_mode,
      'sheet_scope',v_timesheet.sheet_scope,'actual_schedule_json',v_timesheet.actual_schedule_json,
      'additional_units_week',v_timesheet.additional_units_week,'qr_status',v_timesheet.qr_status,
      'canonical_work_date',case when v_timesheet.sheet_scope='DAILY' then private._candidate_daily_work_date_v1(
        v_timesheet.worked_start_iso,v_timesheet.scheduled_start_iso,v_timesheet.week_ending_date) else null end
    ) end,
    'hours',jsonb_build_object('total_hours',coalesce(v_fin.total_hours,0),'actual_schedule_json',v_fin.actual_schedule_json),
    'expenses',jsonb_build_object(
      'expenses_pay_ex_vat',coalesce(v_fin.expenses_pay_ex_vat,0),'expenses_description',v_fin.expenses_description,
      'mileage_units',coalesce(v_fin.mileage_units,0),'mileage_pay_ex_vat',coalesce(v_fin.mileage_pay_ex_vat,0),
      'travel_pay_ex_vat',coalesce(v_fin.travel_pay_ex_vat,0),
      'accommodation_pay_ex_vat',coalesce(v_fin.accommodation_pay_ex_vat,0),
      'other_pay_ex_vat',coalesce(v_fin.other_pay_ex_vat,0)
    ),
    'lifecycle',jsonb_build_object(
      'processing_status',v_fin.processing_status,'authorised_at_utc',v_fin.authorised_at_utc,
      'paid_at_utc',v_fin.paid_at_utc,'invoice_locked',v_fin.locked_by_invoice_id is not null
    ),
    'capabilities',v_capabilities,
    'manager_review',coalesce(v_document_state,'{}'::jsonb),
    'evidence',v_evidence,
    'components',v_components,
    'workflows',v_claims,
    'rejections',(
      select coalesce(jsonb_agg(workflow_item order by
        workflow_item->>'updated_at_utc' desc,workflow_item->>'workflow_id'),'[]'::jsonb)
      from jsonb_array_elements(v_claims) workflow_item
      where workflow_item->>'state'='REJECTED'
        and coalesce((workflow_item->>'rejection_actionable')::boolean,false)
    )
  );
end;
$function$;

create or replace function public.candidate_missing_week_options_v1(
  p_session_id uuid,
  p_environment text,
  p_contract_id uuid,
  p_from date,
  p_to date,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_context jsonb;
  v_candidate_id uuid;
  v_contract public.contracts%rowtype;
  v_first date;
  v_options jsonb;
  v_import_authority jsonb;
begin
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_reads');
  if p_from is null or p_to is null or p_from>p_to or p_to-p_from>366 then
    raise exception 'CANDIDATE_MISSING_WEEK_RANGE_INVALID' using errcode='22023';
  end if;
  v_context:=private._candidate_session_context_v1(p_session_id,p_environment,null,p_now_utc,false);
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  select * into v_contract from public.contracts where id=p_contract_id and candidate_id=v_candidate_id;
  if not found then raise exception 'CANDIDATE_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;
  v_import_authority:=private._candidate_import_authoritative_v1(
    v_contract.client_id,v_contract.id,null,null,p_from
  );
  if coalesce((v_import_authority->>'is_import_authoritative')::boolean,false) then
    return jsonb_build_object('ok',true,'contract_id',p_contract_id,'options','[]'::jsonb,'route_eligible',false);
  end if;
  v_first:=p_from+mod(v_contract.week_ending_weekday_snapshot-extract(dow from p_from)::integer+7,7);
  select coalesce(jsonb_agg(jsonb_build_object(
    'week_ending_date',g::date,'submission_mode',resolved.submission_mode,
    'route_family',case
      when resolved.submission_mode='ELECTRONIC' then 'ELECTRONIC'
      when coalesce((resolved.policy->>'paper_submission_enabled')::boolean,false) then 'QR'
      else 'MANUAL_NON_QR' end,
    'paper_submission_enabled',coalesce((resolved.policy->>'paper_submission_enabled')::boolean,false)
  ) order by g),'[]'::jsonb) into v_options
  from generate_series(v_first,least(p_to,v_contract.end_date),interval '7 days') g
  cross join lateral (
    select
      private._candidate_policy_resolve_v1(v_contract.client_id,v_contract.id,g::date) as policy,
      private._candidate_submission_mode_v1(v_contract.client_id,v_contract.id,g::date) as submission_mode
  ) resolved
  where g::date between v_contract.start_date and v_contract.end_date
    and (
      resolved.submission_mode='ELECTRONIC'
      or (resolved.submission_mode='MANUAL'
        and coalesce((resolved.policy->>'paper_submission_enabled')::boolean,false))
    )
    and not exists(select 1 from public.contract_weeks cw where cw.contract_id=v_contract.id and cw.week_ending_date=g::date and cw.additional_seq=0);
  return jsonb_build_object('ok',true,'contract_id',p_contract_id,'options',v_options,
    'route_eligible',jsonb_array_length(v_options)>0);
end;
$function$;

create or replace function public.candidate_contract_week_add_missing_atomic_v1(
  p_session_id uuid,
  p_environment text,
  p_contract_id uuid,
  p_week_ending_date date,
  p_idempotency_key text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_context jsonb;
  v_candidate_id uuid;
  v_contract public.contracts%rowtype;
  v_week public.contract_weeks%rowtype;
  v_policy jsonb;
  v_mode public.submission_mode_enum;
  v_import_authority jsonb;
begin
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_writes');
  if p_week_ending_date is null or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'CANDIDATE_MISSING_WEEK_PAYLOAD_INVALID' using errcode='22023';
  end if;
  v_context:=private._candidate_session_context_v1(p_session_id,p_environment,null,p_now_utc,true);
  v_candidate_id:=nullif(v_context->>'selected_candidate_id','')::uuid;
  if v_candidate_id is null then raise exception 'CANDIDATE_SELECTION_REQUIRED' using errcode='28000'; end if;
  select * into v_contract from public.contracts where id=p_contract_id and candidate_id=v_candidate_id for update;
  if not found then raise exception 'CANDIDATE_CONTRACT_NOT_FOUND' using errcode='P0002'; end if;
  if p_week_ending_date not between v_contract.start_date and v_contract.end_date
     or extract(dow from p_week_ending_date)::integer<>v_contract.week_ending_weekday_snapshot then
    raise exception 'CANDIDATE_MISSING_WEEK_DATE_INVALID' using errcode='22023';
  end if;
  v_import_authority:=private._candidate_import_authoritative_v1(
    v_contract.client_id,v_contract.id,null,null,p_week_ending_date
  );
  if coalesce((v_import_authority->>'is_import_authoritative')::boolean,false) then
    raise exception 'CANDIDATE_IMPORT_WEEK_CREATION_FORBIDDEN' using errcode='55000';
  end if;
  v_policy:=private._candidate_policy_resolve_v1(v_contract.client_id,v_contract.id,p_week_ending_date);
  v_mode:=private._candidate_submission_mode_v1(v_contract.client_id,v_contract.id,p_week_ending_date);
  if not (v_mode='ELECTRONIC' or (v_mode='MANUAL'
    and coalesce((v_policy->>'paper_submission_enabled')::boolean,false))) then
    raise exception 'CANDIDATE_MISSING_WEEK_ROUTE_NOT_ALLOWED' using errcode='55000';
  end if;
  perform pg_advisory_xact_lock(hashtext(v_contract.id::text||'|'||p_week_ending_date::text||'|BASE_WEEK'));
  select * into v_week from public.contract_weeks
  where contract_id=v_contract.id and week_ending_date=p_week_ending_date and additional_seq=0 for update;
  if found then
    return jsonb_build_object('ok',true,'idempotent_replay',true,'contract_week_id',v_week.id,
      'week_ending_date',v_week.week_ending_date,'status',v_week.status);
  end if;
  insert into public.contract_weeks(
    contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,
    day_entries_json,totals_json,planned_schedule_json,created_at,updated_at
  ) values (
    v_contract.id,p_week_ending_date,0,
    case when p_week_ending_date<=(p_now_utc at time zone 'Europe/London')::date then 'OPEN' else 'PLANNED' end,
    v_mode,'[]'::jsonb,'{}'::jsonb,
    private._candidate_week_schedule_from_template_v1(
      v_contract.std_schedule_json,p_week_ending_date,v_contract.start_date,v_contract.end_date
    ),p_now_utc,p_now_utc
  ) returning * into v_week;
  perform private._candidate_audit_v1('contract_week',v_week.id::text,'CANDIDATE_MISSING_WEEK_CREATED',null,
    jsonb_build_object('contract_id',v_contract.id,'week_ending_date',p_week_ending_date,'status',v_week.status),
    null,null,p_idempotency_key,p_now_utc);
  return jsonb_build_object('ok',true,'idempotent_replay',false,'contract_week_id',v_week.id,
    'week_ending_date',v_week.week_ending_date,'status',v_week.status,'submission_mode',v_mode);
exception when unique_violation then
  select * into v_week from public.contract_weeks
  where contract_id=p_contract_id and week_ending_date=p_week_ending_date and additional_seq=0;
  if found then return jsonb_build_object('ok',true,'idempotent_replay',true,'contract_week_id',v_week.id,
    'week_ending_date',v_week.week_ending_date,'status',v_week.status); end if;
  raise;
end;
$function$;

revoke all on function public.candidate_app_bootstrap_v1(uuid,text,integer,timestamptz) from public,anon,authenticated;
revoke all on function private._candidate_rejection_replaced_v1(uuid) from public,anon,authenticated;
revoke all on function private._candidate_status_code_v1(boolean,boolean,boolean,text,boolean,text,text)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_week_ending_label_v1(date)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_workflow_maps_to_card_v1(uuid,uuid,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_paper_pack_readiness_v1(uuid,integer)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_action_invocation_v1(jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_timesheet_primary_action_v1(text,jsonb,jsonb,uuid,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_timesheet_action_contract_v1(text,jsonb,jsonb,uuid,uuid,timestamptz)
  from public,anon,authenticated,service_role;
revoke all on function public.candidate_app_timesheet_page_v1(uuid,text,text,text,integer,timestamptz)
  from public,anon,authenticated;
revoke all on function public.candidate_app_timesheet_detail_v1(uuid,text,uuid,uuid,uuid,timestamptz) from public,anon,authenticated;
revoke all on function public.candidate_missing_week_options_v1(uuid,text,uuid,date,date,timestamptz) from public,anon,authenticated;
revoke all on function public.candidate_contract_week_add_missing_atomic_v1(uuid,text,uuid,date,text,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_app_bootstrap_v1(uuid,text,integer,timestamptz) to service_role;
grant execute on function public.candidate_app_timesheet_page_v1(uuid,text,text,text,integer,timestamptz)
  to service_role;
grant execute on function public.candidate_app_timesheet_detail_v1(uuid,text,uuid,uuid,uuid,timestamptz) to service_role;
grant execute on function public.candidate_missing_week_options_v1(uuid,text,uuid,date,date,timestamptz) to service_role;
grant execute on function public.candidate_contract_week_add_missing_atomic_v1(uuid,text,uuid,date,text,timestamptz) to service_role;
