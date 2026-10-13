-- DY07: one complete current Office adapter, unchanged permissions/context/replay.
-- Both finalise branches use existing currentSP12 single-flight; no new finalisation process.
\set ON_ERROR_STOP on
begin;

create or replace function public.cloudtms_office_candidate_adapter_v1(
  p_action text,
  p_actor_user_id uuid,
  p_environment text,
  p_payload jsonb default '{}'::jsonb,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_environment text;
  v_payload jsonb:=coalesce(p_payload,'{}'::jsonb);
  v_observed timestamptz:=coalesce(p_now_utc,now());
  v_items jsonb;
  v_results jsonb:='[]'::jsonb;
  v_item jsonb;
  v_result jsonb;
  v_retry_result jsonb;
  v_retry_receipt jsonb;
  v_preview jsonb;
  v_context jsonb;
  v_context_sha text;
  v_batch_id uuid;
  v_idempotency_key text;
  v_request_hash text;
  v_client_request_hash text;
  v_existing_before jsonb;
  v_existing_after jsonb;
  v_workflow_action text;
  v_office_permission text;
  v_workflow_id uuid;
  v_generation integer;
  v_approval_id uuid;
  v_approval_generation integer;
  v_error_code text;
  v_success_count integer:=0;
  v_failure_count integer:=0;
  v_skipped_count integer:=0;
  v_selection_fingerprint text;
  v_current_fingerprint text;
  v_timesheet public.timesheets%rowtype;
  v_family_key text;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  if p_actor_user_id is null or jsonb_typeof(v_payload)<>'object' then
    raise exception 'OFFICE_AUTH_REQUIRED' using errcode='28000';
  end if;
  if v_action='CAPABILITIES' then
    return private._candidate_office_capabilities_v1(v_environment,p_actor_user_id,v_observed);
  end if;
  if v_action='PROJECT_ONE' then
    return private._candidate_office_timesheet_projection_v1(
      v_environment,nullif(v_payload->>'timesheet_id','')::uuid,
      nullif(v_payload->>'contract_week_id','')::uuid,v_payload->>'row_key',
      v_payload->>'expected_row_signature',p_actor_user_id,v_observed
    );
  elsif v_action='PROJECT_BATCH' then
    if upper(coalesce(v_payload->>'surface','')) not in (
      'SIMPLE_TIMESHEET','TIMESHEET_SUMMARY','BULK_PROCESS','BULK_AUTHORISE','INVOICE_GENERATOR','INVOICE_ISSUER'
    ) then
      raise exception 'CANDIDATE_OFFICE_PROJECTION_SURFACE_INVALID' using errcode='22023';
    end if;
    v_items:=coalesce(v_payload->'identities','[]'::jsonb);
    if jsonb_typeof(v_items)<>'array' or jsonb_array_length(v_items)<1 or jsonb_array_length(v_items)>100 then
      raise exception 'CANDIDATE_OFFICE_PROJECTION_BATCH_INVALID' using errcode='22023';
    end if;
    if (select count(*)<>count(distinct coalesce(value->>'row_key',value->>'timesheet_id',value->>'contract_week_id'))
        from jsonb_array_elements(v_items)) then
      raise exception 'CANDIDATE_OFFICE_PROJECTION_DUPLICATE_IDENTITY' using errcode='22023';
    end if;
    for v_item in select value from jsonb_array_elements(v_items) with ordinality x(value,ordinality)
    loop
      begin
        v_result:=private._candidate_office_timesheet_projection_v1(
          v_environment,nullif(v_item->>'timesheet_id','')::uuid,
          nullif(v_item->>'contract_week_id','')::uuid,v_item->>'row_key',
          v_item->>'expected_row_signature',p_actor_user_id,v_observed
        );
        v_results:=v_results||jsonb_build_array(jsonb_build_object(
          'ok',true,'correlation_key',coalesce(v_item->>'row_key',v_item->>'timesheet_id',v_item->>'contract_week_id'),
          'projection',v_result
        ));
      exception when others then
        v_error_code:=coalesce(nullif(substring(sqlerrm from '([A-Z][A-Z0-9_]{2,})'),''),'CANDIDATE_OFFICE_PROJECTION_FAILED');
        v_results:=v_results||jsonb_build_array(jsonb_build_object(
          'ok',false,'correlation_key',coalesce(v_item->>'row_key',v_item->>'timesheet_id',v_item->>'contract_week_id'),
          'error',jsonb_build_object('code',v_error_code,'retryable',sqlstate in ('40001','40P01'))
        ));
      end;
    end loop;
    return jsonb_build_object(
      'ok',true,'contract_version','OFFICE_CANDIDATE_PROJECTION_BATCH_V1',
      'surface',upper(coalesce(v_payload->>'surface','')),
      'observed_at_utc',v_observed,'result_count',jsonb_array_length(v_results),'results',v_results
    );
  elsif v_action='REJECT_PREVIEW' then
    return private._candidate_office_reject_preview_v1(
      v_environment,nullif(v_payload->>'timesheet_id','')::uuid,p_actor_user_id,v_observed
    );
  elsif v_action='REJECT_CONFIRM' then
    v_idempotency_key:=nullif(btrim(coalesce(v_payload->>'idempotency_key','')),'');
    if v_idempotency_key is null or coalesce(v_payload->>'context_sha256','') !~ '^[0-9a-fA-F]{64}$'
       or nullif(btrim(coalesce(v_payload->>'reason','')),'') is null
       or length(btrim(v_payload->>'reason'))>1000 then
      raise exception 'CANDIDATE_REJECT_PAYLOAD_INVALID' using errcode='22023';
    end if;
    v_request_hash:=encode(extensions.digest(convert_to((v_payload-'request_id')::text,'UTF8'),'sha256'),'hex');
    select ae.before_json,ae.after_json into v_existing_before,v_existing_after
    from public.audit_events ae where ae.object_type='cloudtms_office_candidate_rejection'
      and ae.actor_user_id=p_actor_user_id and ae.correlation_id=v_idempotency_key
    order by ae.ts_utc desc,ae.id desc limit 1;
    if found then
      if v_existing_before->>'request_sha256' is distinct from v_request_hash then
        raise exception 'IDEMPOTENCY_CONFLICT' using errcode='23505';
      end if;
      return coalesce(v_existing_after,'{}'::jsonb)||jsonb_build_object('idempotent_replay',true);
    end if;
    select * into v_timesheet from public.timesheets
    where timesheet_id=nullif(v_payload->>'timesheet_id','')::uuid;
    if not found then raise exception 'TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;
    v_family_key:='CANDIDATE_PAPER_FAMILY:'||v_environment||':'
      ||coalesce(v_timesheet.contract_id::text,'-')||':'||coalesce(v_timesheet.week_ending_date::text,'-');
    perform pg_advisory_xact_lock(hashtextextended(v_family_key,0));
    select ae.before_json,ae.after_json into v_existing_before,v_existing_after
    from public.audit_events ae where ae.object_type='cloudtms_office_candidate_rejection'
      and ae.actor_user_id=p_actor_user_id and ae.correlation_id=v_idempotency_key
    order by ae.ts_utc desc,ae.id desc limit 1;
    if found then
      if v_existing_before->>'request_sha256' is distinct from v_request_hash then
        raise exception 'IDEMPOTENCY_CONFLICT' using errcode='23505';
      end if;
      return coalesce(v_existing_after,'{}'::jsonb)||jsonb_build_object('idempotent_replay',true);
    end if;
    perform 1 from public.timesheets where timesheet_id=v_timesheet.timesheet_id for update;
    v_preview:=private._candidate_office_reject_preview_v1(
      v_environment,v_timesheet.timesheet_id,p_actor_user_id,v_observed
    );
    if not coalesce((v_preview->>'permitted')::boolean,false)
       or lower(v_preview->>'context_sha256') is distinct from lower(v_payload->>'context_sha256')
       or v_preview->>'expected_row_signature' is distinct from v_payload->>'expected_row_signature'
       or v_preview->>'expected_timesheet_id' is distinct from coalesce(v_payload->>'expected_timesheet_id',v_payload->>'timesheet_id') then
      raise exception 'CANDIDATE_CONTEXT_STALE' using errcode='40001',detail=v_preview::text;
    end if;
    perform private._candidate_office_service_context_open_v1(
      v_environment,p_actor_user_id,'reject_submission','REJECT_CONFIRM',v_observed
    );
    v_result:=public.candidate_submission_reject_atomic_v1(
      p_actor_user_id,v_environment,v_timesheet.timesheet_id,
      (v_preview->>'expected_timesheet_id')::uuid,v_preview->>'expected_row_signature',
      btrim(v_payload->>'reason'),v_idempotency_key,v_observed
    );
    perform private._candidate_office_service_context_close_v1();
    v_result:=v_result||jsonb_build_object(
      'contract_version','OFFICE_CANDIDATE_MUTATION_RESULT_V1',
      'context_sha256',v_preview->>'context_sha256',
      'refresh_hints',jsonb_build_object(
        'summary',true,'simple_timesheet',true,'bulk_process',true,'bulk_authorise',true,
        'refetch','AFFECTED_ROWS'
      )
    );
    insert into public.audit_events(actor_user_id,object_type,object_id_text,action,before_json,after_json,reason,correlation_id,ts_utc)
    values(p_actor_user_id,'cloudtms_office_candidate_rejection',v_timesheet.timesheet_id::text,
      'CANDIDATE_OFFICE_REJECTION_CONFIRMED',jsonb_build_object('request_sha256',v_request_hash),
      v_result,btrim(v_payload->>'reason'),v_idempotency_key,v_observed);
    return v_result;
  elsif v_action='ROUTE_CONFIRM' then
    perform private._candidate_office_service_context_open_v1(
      v_environment,p_actor_user_id,'change_route','ROUTE_CONFIRM',v_observed
    );
    v_result:=public.timesheet_route_version_confirmed_v1(
      nullif(v_payload->>'current_timesheet_id','')::uuid,
      nullif(v_payload->>'expected_timesheet_id','')::uuid,
      v_payload->>'expected_row_signature',
      v_payload->>'expected_context_sha256',
      v_payload->>'target_action',
      p_actor_user_id,
      v_payload->>'reason_code',
      v_payload->>'reason_note',
      v_payload->>'idempotency_key',
      coalesce((v_payload->>'allow_manual_only')::boolean,false),
      v_observed
    )||jsonb_build_object('contract_version','OFFICE_CANDIDATE_MUTATION_RESULT_V1');
    perform private._candidate_office_service_context_close_v1();
    return v_result;
  elsif v_action in ('FINALISE_EXECUTE','FINALISE_REPLAY_LOOKUP') then
    v_workflow_id:=nullif(v_payload->>'workflow_id','')::uuid;
    v_generation:=nullif(v_payload->>'generation','')::integer;
    v_idempotency_key:=nullif(btrim(coalesce(v_payload->>'idempotency_key','')),'');
    if v_workflow_id is null or v_generation is null or v_generation<1
       or v_idempotency_key is null then
      raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';
    end if;
    perform private._candidate_office_service_context_open_v1(
      v_environment,p_actor_user_id,'retry_finalisation','RETRY_FINALISATION',v_observed
    );
    v_result:=public.candidate_submission_finalize_single_flight_v1(
      null,v_environment,v_workflow_id,v_generation,
      v_payload->>'expected_row_signature',v_idempotency_key,v_observed,
      coalesce(v_payload->'daily_materialisation_json','{}'::jsonb)||jsonb_build_object(
        'service_finalisation',
        coalesce(v_payload->'daily_materialisation_json'->'service_finalisation','{}'::jsonb)
          ||jsonb_build_object(
            'actor_user_id',p_actor_user_id,
            'replay_probe_only',v_action='FINALISE_REPLAY_LOOKUP'
              and not coalesce((v_payload->>'replay_key_probe_only')::boolean,false),
            'replay_key_probe_only',coalesce(
              (v_payload->>'replay_key_probe_only')::boolean,false
            )
          )
      )
    )||jsonb_build_object('contract_version','OFFICE_CANDIDATE_MUTATION_RESULT_V1');
    perform private._candidate_office_service_context_close_v1();
    return v_result;
  elsif v_action in ('PAPER_RETRY_REPLAY','PAPER_RETRY_RECORD') then
    v_workflow_id:=nullif(v_payload->>'workflow_id','')::uuid;
    v_generation:=nullif(v_payload->>'generation','')::integer;
    v_idempotency_key:=nullif(btrim(coalesce(v_payload->>'idempotency_key','')),'');
    if v_workflow_id is null or v_generation is null or v_generation<1
       or v_idempotency_key is null or length(v_idempotency_key)>200 then
      raise exception 'CANDIDATE_PAPER_RETRY_PAYLOAD_INVALID' using errcode='22023';
    end if;
    return private._candidate_office_paper_retry_receipt_v1(
      p_actor_user_id,v_workflow_id,v_generation,v_idempotency_key,
      case when v_action='PAPER_RETRY_RECORD'
        then nullif(v_payload->>'http_status','')::integer else null end,
      case when v_action='PAPER_RETRY_RECORD' then v_payload->'result' else null end,
      v_action='PAPER_RETRY_REPLAY',v_observed
    );
  elsif v_action='WORKFLOW_ACTION_EXECUTE' then
    v_workflow_action:=upper(btrim(coalesce(v_payload->>'workflow_action','')));
    if v_workflow_action not in ('REMIND','RENEW','MANAGER_REQUEST_CANCEL','CANCEL_MANAGER_HANDOFF',
        'BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS','PHONE_APPROVE','MANAGER_REFUSE',
        'REGISTER_REVIEW_COMPONENT','REGISTER_FINAL_SIGNED_DOCUMENT','BEGIN_CANONICAL_DAILY_SAVE',
        'PAPER_PACK_RELEASE','PAPER_PACK_ATTEMPT_CLAIM','PAPER_PACK_MARK_FAILURE') then
      raise exception 'CANDIDATE_WORKFLOW_ACTION_INVALID' using errcode='22023';
    end if;
    v_workflow_id:=nullif(v_payload->>'workflow_id','')::uuid;
    v_generation:=nullif(v_payload->>'generation','')::integer;
    v_idempotency_key:=nullif(btrim(coalesce(v_payload->>'idempotency_key','')),'');
    if v_workflow_id is null or v_generation is null or v_generation<1 or v_idempotency_key is null then
      raise exception 'CANDIDATE_WORKFLOW_PAYLOAD_INVALID' using errcode='22023';
    end if;
    v_request_hash:=encode(extensions.digest(convert_to((v_payload-'request_id')::text,'UTF8'),'sha256'),'hex');
    select ae.before_json,ae.after_json into v_existing_before,v_existing_after
    from public.audit_events ae where ae.object_type='cloudtms_office_candidate_action'
      and ae.object_id_text=v_workflow_id::text and ae.actor_user_id=p_actor_user_id
      and ae.correlation_id=v_idempotency_key
    order by ae.ts_utc desc,ae.id desc limit 1;
    if found then
      if v_existing_before->>'request_sha256' is distinct from v_request_hash then
        raise exception 'IDEMPOTENCY_CONFLICT' using errcode='23505';
      end if;
      v_result:=coalesce(v_existing_after,'{}'::jsonb);
      if v_workflow_action='PAPER_PACK_ATTEMPT_CLAIM' then
        v_result:=v_result||jsonb_build_object('claim_acquired_new',false);
      end if;
      return v_result||jsonb_build_object('idempotent_replay',true);
    end if;
    v_approval_id:=nullif(v_payload->>'approval_request_id','')::uuid;
    v_approval_generation:=nullif(v_payload->>'approval_request_generation','')::integer;
    if v_workflow_action in ('REMIND','RENEW','MANAGER_REQUEST_CANCEL','CANCEL_MANAGER_HANDOFF',
        'BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS','PHONE_APPROVE','MANAGER_REFUSE')
       and (v_approval_id is null or v_approval_generation is null or v_approval_generation<1) then
      raise exception 'CANDIDATE_REQUEST_GENERATION_STALE' using errcode='22023';
    end if;
    if v_approval_id is not null then
      perform 1 from public.candidate_approval_requests ar
      where ar.id=v_approval_id and ar.workflow_id=v_workflow_id
        and ar.workflow_generation=v_generation and ar.request_generation=v_approval_generation;
      if not found then raise exception 'CANDIDATE_REQUEST_GENERATION_STALE' using errcode='40001'; end if;
    end if;
    v_office_permission:=case
      when v_workflow_action='REMIND' then 'send_manager_reminder'
      when v_workflow_action='RENEW' then 'renew_manager_request'
      when v_workflow_action in ('MANAGER_REQUEST_CANCEL','CANCEL_MANAGER_HANDOFF') then 'cancel_manager_request'
      when v_workflow_action in ('BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS','PHONE_APPROVE','MANAGER_REFUSE')
        then 'manage_phone_approval'
      when v_workflow_action in ('REGISTER_REVIEW_COMPONENT','REGISTER_FINAL_SIGNED_DOCUMENT','BEGIN_CANONICAL_DAILY_SAVE')
        then 'retry_finalisation'
      when v_workflow_action in ('PAPER_PACK_RELEASE','PAPER_PACK_ATTEMPT_CLAIM','PAPER_PACK_MARK_FAILURE')
        then 'manage_paper'
      else null end;
    perform private._candidate_office_service_context_open_v1(
      v_environment,p_actor_user_id,v_office_permission,v_workflow_action,v_observed
    );
    v_result:=public.candidate_workflow_transition_atomic_v1(
      null,v_environment,v_workflow_id,v_workflow_action,v_generation,
      coalesce(v_payload->'payload','{}'::jsonb)||jsonb_build_object(
        'service_office_action',true,'actor_user_id',p_actor_user_id,
        'approval_request_id',v_approval_id,'approval_request_generation',v_approval_generation
      ),v_idempotency_key,v_observed
    );
    perform private._candidate_office_service_context_close_v1();
    if v_workflow_action in ('PAPER_PACK_RELEASE','PAPER_PACK_MARK_FAILURE')
       and nullif(btrim(coalesce(
         v_payload#>>'{payload,paper_pack_operation_id}',''
       )),'') is not null then
      v_idempotency_key:=btrim(v_payload#>>'{payload,paper_pack_operation_id}');
      if v_workflow_action='PAPER_PACK_RELEASE' then
        v_retry_result:=jsonb_build_object(
          'ok',true,
          'contract_version','OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3',
          'idempotency_key',v_idempotency_key,
          'workflow_id',v_workflow_id,
          'generation',v_generation,
          'paper_pack_state','READY',
          'page_count',coalesce(
            nullif(v_result->>'complete_pack_page_count','')::integer,0
          ),
          'release',jsonb_build_object(
            'ok',coalesce((v_result->>'ok')::boolean,false),
            'workflow_id',v_workflow_id,
            'generation',v_generation,
            'state',v_result->>'state',
            'timesheet_id',v_result->'timesheet_id',
            'mail_outbox_id',v_result->'mail_outbox_id',
            'notification_id',v_result->'notification_id',
            'complete_pack_page_count',coalesce(
              nullif(v_result->>'complete_pack_page_count','')::integer,0
            ),
            'idempotent_replay',coalesce((v_result->>'idempotent_replay')::boolean,false)
          )
        );
        v_retry_receipt:=private._candidate_office_paper_retry_receipt_v1(
          p_actor_user_id,v_workflow_id,v_generation,v_idempotency_key,
          200,v_retry_result,false,v_observed
        );
      else
        v_retry_result:=jsonb_build_object(
          'ok',false,
          'contract_version','OFFICE_CANDIDATE_PAPER_RETRY_RESULT_V3',
          'idempotency_key',v_idempotency_key,
          'workflow_id',v_workflow_id,
          'generation',v_generation,
          'paper_pack_state',v_result->>'paper_pack_state',
          'retryable',coalesce((v_result->>'retryable')::boolean,false),
          'error_code',v_result->>'failure_code',
          'next_retry_at_utc',v_result->'next_retry_at_utc'
        );
        v_retry_receipt:=private._candidate_office_paper_retry_receipt_v1(
          p_actor_user_id,v_workflow_id,v_generation,v_idempotency_key,
          case when coalesce((v_result->>'retryable')::boolean,false) then 503 else 409 end,
          v_retry_result,false,v_observed
        );
      end if;
      v_result:=v_result||jsonb_build_object(
        'office_paper_retry_receipt',v_retry_receipt
      );
    end if;
    v_result:=v_result||jsonb_build_object(
      'contract_version','OFFICE_CANDIDATE_MUTATION_RESULT_V1',
      'refresh_hints',jsonb_build_object(
        'summary',true,'simple_timesheet',true,'bulk_process',true,'bulk_authorise',true,
        'refetch','AFFECTED_ROWS'
      )
    );
    insert into public.audit_events(actor_user_id,object_type,object_id_text,action,before_json,after_json,reason,correlation_id,ts_utc)
    values(p_actor_user_id,'cloudtms_office_candidate_action',v_workflow_id::text,
      'CANDIDATE_OFFICE_'||v_workflow_action,jsonb_build_object('request_sha256',v_request_hash),
      v_result,null,v_idempotency_key,v_observed);
    return v_result;
  elsif v_action='REMINDER_BATCH_REPLAY' then
    v_items:=coalesce(v_payload->'identities','[]'::jsonb);
    v_batch_id:=nullif(v_payload->>'batch_id','')::uuid;
    v_idempotency_key:=nullif(btrim(coalesce(v_payload->>'idempotency_key','')),'');
    if jsonb_typeof(v_items)<>'array' or jsonb_array_length(v_items)<1 or jsonb_array_length(v_items)>1000
       or v_batch_id is null or v_idempotency_key is null or v_batch_id::text<>v_idempotency_key
       or coalesce(v_payload->>'preview_context_hash','') !~ '^[0-9a-fA-F]{64}$'
       or coalesce(v_payload->>'selection_fingerprint','') !~ '^[0-9a-fA-F]{64}$' then
      raise exception 'CANDIDATE_REMINDER_BATCH_SELECTION_INVALID' using errcode='22023';
    end if;
    if (select count(*)<>count(distinct coalesce(value->>'row_key',value->>'timesheet_id',value->>'contract_week_id'))
        from jsonb_array_elements(v_items)) then
      raise exception 'CANDIDATE_REMINDER_BATCH_DUPLICATE_IDENTITY' using errcode='22023';
    end if;
    v_client_request_hash:=encode(extensions.digest(convert_to(jsonb_build_object(
      'identities',v_items,'batch_id',v_batch_id,'idempotency_key',v_idempotency_key,
      'preview_context_hash',lower(v_payload->>'preview_context_hash'),
      'selection_fingerprint',lower(v_payload->>'selection_fingerprint')
    )::text,'UTF8'),'sha256'),'hex');
    select ae.before_json,ae.after_json into v_existing_before,v_existing_after
    from public.audit_events ae where ae.object_type='cloudtms_office_candidate_reminder_batch'
      and ae.object_id_text=v_batch_id::text and ae.actor_user_id=p_actor_user_id
    order by ae.ts_utc desc,ae.id desc limit 1;
    if not found then
      return jsonb_build_object('ok',true,'found',false,'batch_id',v_batch_id);
    end if;
    if v_existing_before->>'client_request_sha256' is distinct from v_client_request_hash then
      raise exception 'IDEMPOTENCY_CONFLICT' using errcode='23505';
    end if;
    return coalesce(v_existing_after,'{}'::jsonb)||jsonb_build_object('found',true,'idempotent_replay',true);
  elsif v_action in ('REMINDER_BATCH_PREVIEW','REMINDER_BATCH_EXECUTE') then
    v_items:=coalesce(v_payload->'identities','[]'::jsonb);
    if jsonb_typeof(v_items)<>'array' or jsonb_array_length(v_items)<1 or jsonb_array_length(v_items)>1000 then
      raise exception 'CANDIDATE_REMINDER_BATCH_SELECTION_INVALID' using errcode='22023';
    end if;
    if (select count(*)<>count(distinct coalesce(value->>'row_key',value->>'timesheet_id',value->>'contract_week_id'))
        from jsonb_array_elements(v_items)) then
      raise exception 'CANDIDATE_REMINDER_BATCH_DUPLICATE_IDENTITY' using errcode='22023';
    end if;
    if v_action='REMINDER_BATCH_EXECUTE' then
      v_batch_id:=nullif(v_payload->>'batch_id','')::uuid;
      v_idempotency_key:=nullif(btrim(coalesce(v_payload->>'idempotency_key','')),'');
      if v_batch_id is null or v_idempotency_key is null or v_batch_id::text<>v_idempotency_key then
        raise exception 'CANDIDATE_REMINDER_BATCH_SELECTION_CHANGED' using errcode='40001';
      end if;
      v_client_request_hash:=encode(extensions.digest(convert_to(jsonb_build_object(
        'identities',v_items,'batch_id',v_batch_id,'idempotency_key',v_idempotency_key,
        'preview_context_hash',lower(v_payload->>'preview_context_hash'),
        'selection_fingerprint',lower(v_payload->>'selection_fingerprint')
      )::text,'UTF8'),'sha256'),'hex');
      v_request_hash:=encode(extensions.digest(convert_to((v_payload-'request_id')::text,'UTF8'),'sha256'),'hex');
      perform pg_advisory_xact_lock(hashtextextended('CANDIDATE_OFFICE_REMINDER_BATCH:'||p_actor_user_id::text||':'||v_batch_id::text,0));
      select ae.before_json,ae.after_json into v_existing_before,v_existing_after
      from public.audit_events ae where ae.object_type='cloudtms_office_candidate_reminder_batch'
        and ae.object_id_text=v_batch_id::text and ae.actor_user_id=p_actor_user_id
      order by ae.ts_utc desc,ae.id desc limit 1;
      if found then
        if v_existing_before->>'request_sha256' is distinct from v_request_hash then
          raise exception 'IDEMPOTENCY_CONFLICT' using errcode='23505';
        end if;
        return coalesce(v_existing_after,'{}'::jsonb)||jsonb_build_object('idempotent_replay',true);
      end if;
    end if;
    for v_item in select value from jsonb_array_elements(v_items) with ordinality x(value,ordinality)
    loop
      begin
        v_result:=private._candidate_office_timesheet_projection_v1(
          v_environment,nullif(v_item->>'timesheet_id','')::uuid,
          nullif(v_item->>'contract_week_id','')::uuid,v_item->>'row_key',
          v_item->>'expected_row_signature',p_actor_user_id,v_observed
        );
        v_preview:=(select action_item from jsonb_array_elements(v_result->'available_actions') action_item
          where action_item->>'code'='SEND_MANAGER_REMINDER' limit 1);
        v_results:=v_results||jsonb_build_array(jsonb_build_object(
          'correlation_key',coalesce(v_item->>'row_key',v_item->>'timesheet_id',v_item->>'contract_week_id'),
          'eligible',coalesce((v_preview->>'enabled')::boolean,false),
          'disabled_reason_code',v_preview->>'disabled_reason_code',
          'workflow_id',v_result#>>'{workflow,workflow_id}',
          'workflow_generation',v_result#>>'{workflow,generation}',
          'approval_request_id',v_result#>>'{manager_approval,request_id}',
          'approval_request_generation',v_result#>>'{manager_approval,request_generation}',
          'row_signature',v_result#>>'{current_identity,row_signature}'
        ));
      exception when others then
        v_error_code:=coalesce(nullif(substring(sqlerrm from '([A-Z][A-Z0-9_]{2,})'),''),'CANDIDATE_OFFICE_PROJECTION_FAILED');
        v_results:=v_results||jsonb_build_array(jsonb_build_object(
          'correlation_key',coalesce(v_item->>'row_key',v_item->>'timesheet_id',v_item->>'contract_week_id'),
          'eligible',false,'disabled_reason_code',v_error_code
        ));
      end;
    end loop;
    v_selection_fingerprint:=encode(extensions.digest(convert_to(v_results::text,'UTF8'),'sha256'),'hex');
    if v_action='REMINDER_BATCH_PREVIEW' then
      return jsonb_build_object(
        'ok',true,'contract_version','OFFICE_CANDIDATE_REMINDER_BATCH_PREVIEW_V1',
        'batch_version','OFFICE_CANDIDATE_REMINDER_BATCH_V1','observed_at_utc',v_observed,
        'preview_context_hash',v_selection_fingerprint,
        'selection_fingerprint',v_selection_fingerprint,
        'selected_count',jsonb_array_length(v_results),
        'eligible_count',(select count(*) from jsonb_array_elements(v_results) r where (r->>'eligible')::boolean),
        'ineligible_count',(select count(*) from jsonb_array_elements(v_results) r where not (r->>'eligible')::boolean),
        'changed_count',(select count(*) from jsonb_array_elements(v_results) r
          where r->>'disabled_reason_code' in ('TIMESHEET_MOVED','ROW_SIGNATURE_MISMATCH','CANDIDATE_CONTEXT_STALE')),
        'items',v_results
      );
    end if;
    if lower(coalesce(v_payload->>'preview_context_hash','')) is distinct from v_selection_fingerprint
       or lower(coalesce(v_payload->>'selection_fingerprint','')) is distinct from v_selection_fingerprint then
      raise exception 'CANDIDATE_REMINDER_BATCH_SELECTION_CHANGED' using errcode='40001';
    end if;
    v_items:=coalesce(v_payload->'reminders','[]'::jsonb);
    if jsonb_typeof(v_items)<>'array'
       or jsonb_array_length(v_items)<>jsonb_array_length(v_results)
       or exists(
         select 1
         from jsonb_array_elements(v_items) supplied
         where not exists(
           select 1
           from jsonb_array_elements(v_results) current_item
           where current_item->>'correlation_key'=supplied->>'correlation_key'
             and current_item->>'eligible'=supplied->>'eligible'
             and current_item->>'workflow_id' is not distinct from supplied->>'workflow_id'
             and current_item->>'workflow_generation' is not distinct from supplied->>'workflow_generation'
             and current_item->>'approval_request_id' is not distinct from supplied->>'approval_request_id'
             and current_item->>'approval_request_generation' is not distinct from supplied->>'approval_request_generation'
         )
       ) then
      raise exception 'CANDIDATE_REMINDER_BATCH_SELECTION_CHANGED' using errcode='40001';
    end if;
    v_results:='[]'::jsonb;
    for v_item in select value from jsonb_array_elements(v_items) with ordinality x(value,ordinality)
    loop
      if not coalesce((v_item->>'eligible')::boolean,false) then
        v_skipped_count:=v_skipped_count+1;
        v_results:=v_results||jsonb_build_array(v_item||jsonb_build_object('outcome','SKIPPED'));
        continue;
      end if;
      begin
        perform private._candidate_office_service_context_open_v1(
          v_environment,p_actor_user_id,'send_manager_reminder_batch','REMIND',v_observed
        );
        v_result:=public.candidate_workflow_transition_atomic_v1(
          null,v_environment,(v_item->>'workflow_id')::uuid,'REMIND',(v_item->>'workflow_generation')::integer,
          coalesce(v_item->'payload','{}'::jsonb)||jsonb_build_object(
            'service_office_action',true,'actor_user_id',p_actor_user_id,
            'approval_request_id',(v_item->>'approval_request_id')::uuid,
            'approval_request_generation',(v_item->>'approval_request_generation')::integer
          ),'office-reminder-batch:'||v_batch_id::text||':'||(v_item->>'workflow_id')||':'||(v_item->>'approval_request_generation'),v_observed
        );
        perform private._candidate_office_service_context_close_v1();
        v_success_count:=v_success_count+1;
        v_results:=v_results||jsonb_build_array(jsonb_build_object(
          'correlation_key',v_item->>'correlation_key','workflow_id',v_item->>'workflow_id',
          'approval_request_id',v_item->>'approval_request_id','outcome','QUEUED','result',v_result
        ));
      exception when others then
        perform private._candidate_office_service_context_close_v1();
        v_failure_count:=v_failure_count+1;
        v_error_code:=coalesce(nullif(substring(sqlerrm from '([A-Z][A-Z0-9_]{2,})'),''),'CANDIDATE_REMINDER_BATCH_ITEM_FAILED');
        v_results:=v_results||jsonb_build_array(jsonb_build_object(
          'correlation_key',v_item->>'correlation_key','workflow_id',v_item->>'workflow_id',
          'approval_request_id',v_item->>'approval_request_id','outcome','FAILED',
          'error_code',v_error_code,'retryable',sqlstate in ('40001','40P01')
        ));
      end;
    end loop;
    v_result:=jsonb_build_object(
      -- The batch operation itself completed durably even when one or more
      -- independently fenced items failed. Item outcome truth is represented
      -- by status/counts/items; top-level ok=false is reserved for a request or
      -- transaction that did not produce a durable batch result.
      'ok',true,'contract_version','OFFICE_CANDIDATE_REMINDER_BATCH_RESULT_V1',
      'batch_version','OFFICE_CANDIDATE_REMINDER_BATCH_V1','batch_id',v_batch_id,
      'status',case when v_failure_count=0 then 'COMPLETED' when v_success_count>0 then 'PARTIAL' else 'FAILED' end,
      'observed_at_utc',v_observed,'completed_at_utc',v_observed,
      'selection_fingerprint',v_selection_fingerprint,
      'success_count',v_success_count,'failure_count',v_failure_count,'skipped_count',v_skipped_count,
      'items',v_results,'idempotent_replay',false
    );
    insert into public.audit_events(actor_user_id,object_type,object_id_text,action,before_json,after_json,correlation_id,ts_utc)
    values(p_actor_user_id,'cloudtms_office_candidate_reminder_batch',v_batch_id::text,
      'CANDIDATE_OFFICE_REMINDER_BATCH_COMPLETED',jsonb_build_object(
        'request_sha256',v_request_hash,'client_request_sha256',v_client_request_hash
      ),
      v_result,v_idempotency_key,v_observed);
    return v_result;
  elsif v_action='REMINDER_BATCH_STATUS' then
    v_batch_id:=nullif(v_payload->>'batch_id','')::uuid;
    if v_batch_id is null then raise exception 'CANDIDATE_REMINDER_BATCH_NOT_FOUND' using errcode='P0002'; end if;
    select ae.after_json into v_result from public.audit_events ae
    where ae.object_type='cloudtms_office_candidate_reminder_batch'
      and ae.object_id_text=v_batch_id::text and ae.actor_user_id=p_actor_user_id
    order by ae.ts_utc desc,ae.id desc limit 1;
    if not found then raise exception 'CANDIDATE_REMINDER_BATCH_NOT_FOUND' using errcode='P0002'; end if;
    return v_result;
  end if;
  raise exception 'CANDIDATE_OFFICE_ADAPTER_ACTION_INVALID' using errcode='22023';
end;
$function$;

alter function public.cloudtms_office_candidate_adapter_v1(text,uuid,text,jsonb,timestamp with time zone) owner to postgres;
revoke all on function public.cloudtms_office_candidate_adapter_v1(text,uuid,text,jsonb,timestamp with time zone) from public,anon,authenticated;
grant execute on function public.cloudtms_office_candidate_adapter_v1(text,uuid,text,jsonb,timestamp with time zone) to service_role;
notify pgrst,'reload schema';
commit;
