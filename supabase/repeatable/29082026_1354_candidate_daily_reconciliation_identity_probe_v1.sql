-- Repeatable CloudTMS function/view authority: candidate_daily_reconciliation_identity_probe_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

create or replace function public.candidate_daily_reconciliation_apply_atomic_v1(
  p_internal_context jsonb,
  p_batch_request_id uuid,
  p_idempotency_key text,
  p_observations jsonb,
  p_correlation_id text
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_context jsonb;
  v_environment text;
  v_route_operation text;
  v_operation_class text;
  v_request_hash text;
  v_batch private.candidate_daily_batch_receipts%rowtype;
  v_batch_id uuid:=gen_random_uuid();
  v_item_keys jsonb;
  v_item jsonb;
  v_index integer:=0;
  v_candidate_id uuid;
  v_scope private.candidate_daily_authority_scopes%rowtype;
  v_day public.candidate_daily_rota_days%rowtype;
  v_availability public.candidate_daily_availability_days%rowtype;
  v_preference text;
  v_command_id uuid;
  v_command public.candidate_daily_command_receipts%rowtype;
  v_item_hash text;
  v_item_key text;
  v_outcomes jsonb:='[]'::jsonb;
  v_terminal jsonb;
  v_classification text;
  v_error text;
  v_probe_only boolean;
  v_reconciled_candidates uuid[]:='{}'::uuid[];
  v_reconciled_revisions jsonb:='{}'::jsonb;
  v_refresh_candidate_id uuid;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'SIGNED_SYSTEM_SYNC',false);
  v_environment:=v_context->>'environment';
  v_route_operation:=upper(coalesce(nullif(p_internal_context->>'route_operation',''),'RECONCILIATION'));
  if v_route_operation not in ('RECONCILIATION','SHEET_EDIT_INGEST') then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  v_operation_class:=case when v_route_operation='SHEET_EDIT_INGEST' then 'SHEET_EDIT_INGEST' else 'RECONCILIATION' end;
  if p_batch_request_id is null or p_idempotency_key !~ '^[A-Za-z0-9._~:+/-]{16,128}$'
     or jsonb_typeof(p_observations)<>'array' or jsonb_array_length(p_observations) not between 1 and 100
     or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  select jsonb_agg(i->>'item_key' order by ord) into v_item_keys
    from jsonb_array_elements(p_observations) with ordinality x(i,ord);
  if exists(select 1 from jsonb_array_elements(p_observations)i where i->>'item_key' !~ '^[A-Za-z0-9._~-]{8,160}$')
     or jsonb_array_length(v_item_keys)<>(select count(distinct x) from jsonb_array_elements_text(v_item_keys)x) then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  v_request_hash:=private._candidate_daily_json_sha256_v1(jsonb_build_object('operation',v_route_operation,
    'batch_request_id',p_batch_request_id,'items',p_observations));
  insert into private.candidate_daily_batch_receipts(batch_receipt_id,environment,actor_class,operation_class,
    idempotency_key,request_hash,item_keys_json,item_count,state,correlation_id)
  values(v_batch_id,v_environment,'SIGNED_SYSTEM',v_operation_class,p_idempotency_key,v_request_hash,
    v_item_keys,jsonb_array_length(p_observations),'IN_PROGRESS',p_correlation_id)
  on conflict(environment,actor_class,operation_class,idempotency_key) do nothing;
  select * into v_batch from private.candidate_daily_batch_receipts where environment=v_environment
    and actor_class='SIGNED_SYSTEM' and operation_class=v_operation_class and idempotency_key=p_idempotency_key for update;
  if v_batch.request_hash<>v_request_hash then
    raise exception using errcode='23505',message='IDEMPOTENCY_KEY_REUSED';
  end if;
  if v_batch.state<>'IN_PROGRESS' then
    return v_batch.terminal_response_body||jsonb_build_object('_idempotent_replay',true);
  end if;

  for v_item in select value from jsonb_array_elements(p_observations) loop
    v_error:=null;
    v_classification:=null;
    v_probe_only:=v_route_operation='RECONCILIATION' and v_item->'probe_only'='true'::jsonb;
    begin
      if v_probe_only and (
        not(v_item ?& array['candidate_source_hmac','probe_only','source_revision',
          'source_event_time','source_hash','item_key'])
        or (select count(*) from jsonb_object_keys(v_item))<>6
        or jsonb_typeof(v_item->'probe_only')<>'boolean'
        or not (v_item->>'probe_only')::boolean
        or v_item->>'candidate_source_hmac' !~ '^[a-f0-9]{64}$'
        or v_item->>'source_revision' !~ '^[A-Za-z0-9._~:+/-]{1,160}$'
        or v_item->>'source_hash' !~ '^[a-f0-9]{64}$'
        or v_item->>'source_event_time' !~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$'
      ) then
        raise exception using errcode='22023',message='VALIDATION_FAILED';
      end if;
      v_candidate_id:=private._candidate_daily_source_candidate_v1(v_environment,v_item->>'candidate_source_hmac');
      if v_route_operation='SHEET_EDIT_INGEST' then
        if not(v_item ?& array['source_event_id','source_revision','editor_hmac','sheet','cell','date',
          'availability','source_event_time','source_hash','item_key'])
          or v_item->>'sheet'<>'Availability' or v_item->>'cell' !~ '^[A-Z]{1,3}[1-9][0-9]{0,5}$'
          or v_item->>'editor_hmac' !~ '^[a-f0-9]{64}$' or v_item->>'source_hash' !~ '^[a-f0-9]{64}$' then
          raise exception using errcode='22023',message='VALIDATION_FAILED';
        end if;
        v_preference:=private._candidate_daily_preference_v1(v_item->>'availability');
        if v_preference is null then
          raise exception using errcode='22023',message='VALIDATION_FAILED';
        end if;
        v_item_hash:=private._candidate_daily_json_sha256_v1(v_item);
        v_item_key:=left(p_idempotency_key||':item:'||v_item->>'item_key',160);
        v_command_id:=gen_random_uuid();
        select * into v_scope from private.candidate_daily_authority_scopes
          where environment=v_environment and candidate_id=v_candidate_id for update;
        insert into public.candidate_daily_command_receipts(command_id,environment,candidate_id,actor_class,
          command_class,idempotency_key,request_sha256,source_system,source_event_id,source_revision,
          source_event_time,item_key,canonical_version_before,state,correlation_id)
        values(v_command_id,v_environment,v_candidate_id,'SIGNED_SYSTEM','SHEET_EDIT_INGEST',v_item_key,
          v_item_hash,'MASTER_AVAILABILITY_SHEET',v_item->>'source_event_id',v_item->>'source_revision',
          (v_item->>'source_event_time')::timestamptz,v_item->>'item_key',v_scope.canonical_version,
          'IN_PROGRESS',p_correlation_id)
        on conflict do nothing;
        select * into v_command from public.candidate_daily_command_receipts where
          (environment=v_environment and source_system='MASTER_AVAILABILITY_SHEET'
            and source_event_id=v_item->>'source_event_id' and item_key=v_item->>'item_key')
          or (environment=v_environment and candidate_id=v_candidate_id and actor_class='SIGNED_SYSTEM'
            and idempotency_key=v_item_key)
          order by (source_event_id=v_item->>'source_event_id') desc limit 1 for update;
        if v_command.request_sha256<>v_item_hash then
          raise exception using errcode='23505',message='SOURCE_EVENT_CONFLICT';
        end if;
        if v_command.state='COMPLETED' then
          v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'status','REPLAYED',
            'availability_version',v_command.canonical_version_after));
        else
          select * into v_day from public.candidate_daily_rota_days
            where generation_id=v_scope.active_generation_id and rota_date=(v_item->>'date')::date;
          if v_day.generation_id is null or v_day.booked or v_day.system_blocked then
            raise exception using errcode='22023',message='AVAILABILITY_DATE_NOT_EDITABLE';
          end if;
          update private.candidate_daily_authority_scopes set canonical_version=canonical_version+1,updated_at_utc=now()
            where environment=v_environment and candidate_id=v_candidate_id returning canonical_version into v_scope.canonical_version;
          insert into public.candidate_daily_availability_days(environment,candidate_id,availability_date,preference,
            availability_version,source_class,source_command_id,changed_by_class,row_hash)
          values(v_environment,v_candidate_id,(v_item->>'date')::date,v_preference,v_scope.canonical_version,
            'SIGNED_SYSTEM',v_command.command_id,'SIGNED_SYSTEM',v_item_hash)
          on conflict(environment,candidate_id,availability_date) do update set preference=excluded.preference,
            availability_version=excluded.availability_version,source_class=excluded.source_class,
            source_command_id=excluded.source_command_id,changed_by_class=excluded.changed_by_class,
            row_hash=excluded.row_hash,changed_at_utc=now();
          insert into private.candidate_daily_sync_state(environment,candidate_id,target,accepted_canonical_cursor,
            required_visible_cursor,delivered_visible_cursor,overlay_proof_cursor,effective_visible_cursor,
            observed_source_revision,state,last_acknowledged_at_utc)
          values(v_environment,v_candidate_id,'MASTER_AVAILABILITY_SHEET',v_scope.canonical_version,
            v_scope.canonical_version,v_scope.canonical_version,v_scope.canonical_version,v_scope.canonical_version,
            v_item->>'source_revision','READY',now())
          on conflict(environment,candidate_id,target) do update set
            accepted_canonical_cursor=greatest(private.candidate_daily_sync_state.accepted_canonical_cursor,v_scope.canonical_version),
            required_visible_cursor=greatest(private.candidate_daily_sync_state.required_visible_cursor,v_scope.canonical_version),
            delivered_visible_cursor=greatest(private.candidate_daily_sync_state.delivered_visible_cursor,v_scope.canonical_version),
            effective_visible_cursor=greatest(private.candidate_daily_sync_state.effective_visible_cursor,v_scope.canonical_version),
            observed_source_revision=excluded.observed_source_revision,state='READY',last_acknowledged_at_utc=now(),updated_at_utc=now();
          update public.candidate_daily_command_receipts set canonical_version_after=v_scope.canonical_version,
            state='COMPLETED',terminal_http_status=200,
            terminal_body_json=jsonb_build_object('availability_version',v_scope.canonical_version),
            terminal_body_sha256=private._candidate_daily_json_sha256_v1(jsonb_build_object('availability_version',v_scope.canonical_version)),
            completed_at_utc=now(),updated_at_utc=now() where command_id=v_command.command_id;
          v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'status','COMMITTED',
            'availability_version',v_scope.canonical_version));
        end if;
      elsif v_probe_only then
        v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object(
          'index',v_index,'classification','LINKED'));
      else
        if not(v_item ?& array['date','observed_value','observed_sheet_revision','source_event_id',
          'source_revision','source_event_time','source_hash','item_key'])
          or v_item->>'source_hash' !~ '^[a-f0-9]{64}$' then
          raise exception using errcode='22023',message='VALIDATION_FAILED';
        end if;
        v_preference:=private._candidate_daily_preference_v1(v_item->>'observed_value');
        select * into v_availability from public.candidate_daily_availability_days
          where environment=v_environment and candidate_id=v_candidate_id and availability_date=(v_item->>'date')::date;
        if v_preference is null then
          v_classification:='AMBIGUOUS';
        elsif v_availability.candidate_id is null then
          v_classification:='CANONICAL_COMMAND_REQUIRED';
        elsif v_availability.preference=v_preference then
          v_classification:='MATCH';
          update public.candidate_daily_sheet_projection_outbox
          set state='DELIVERED',observed_sheet_revision=v_item->>'observed_sheet_revision',
            overlay_generation_id=null,overlay_generation_version=null,overlay_source_row_hash=null,
            completed_at_utc=now(),lease_owner=null,lease_token=null,lease_expires_at_utc=null,
            updated_at_utc=now()
          where environment=v_environment and candidate_id=v_candidate_id
            and target='MASTER_AVAILABILITY_SHEET'
            and availability_date=v_availability.availability_date
            and availability_version=v_availability.availability_version
            and preference=v_availability.preference and state<>'TERMINAL';
        else
          insert into public.candidate_daily_sheet_projection_outbox(environment,candidate_id,availability_date,
            availability_version,preference,command_id,correlation_id)
          values(v_environment,v_candidate_id,v_availability.availability_date,v_availability.availability_version,
            v_availability.preference,v_availability.source_command_id,p_correlation_id)
          on conflict(environment,target,candidate_id,availability_date,availability_version,operation)
          do update set state=case when public.candidate_daily_sheet_projection_outbox.state='TERMINAL'
            then 'TERMINAL' else 'PENDING' end,next_available_at_utc=now(),
            overlay_generation_id=null,overlay_generation_version=null,overlay_source_row_hash=null,
            completed_at_utc=null,updated_at_utc=now();
          v_classification:='REPAIR_PROJECTION';
        end if;
        if not v_candidate_id=any(v_reconciled_candidates) then
          v_reconciled_candidates:=array_append(v_reconciled_candidates,v_candidate_id);
        end if;
        v_reconciled_revisions:=jsonb_set(v_reconciled_revisions,array[v_candidate_id::text],
          to_jsonb(v_item->>'observed_sheet_revision'),true);
        v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'classification',v_classification));
      end if;
    exception when others then
      v_error:=sqlerrm;
      if v_route_operation='SHEET_EDIT_INGEST' and v_error in ('SOURCE_EVENT_CONFLICT','AVAILABILITY_DATE_NOT_EDITABLE',
        'IDENTITY_LINK_MISSING','IDENTITY_LINK_AMBIGUOUS','VALIDATION_FAILED') then
        v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'status','REJECTED','error_code',v_error));
      elsif v_route_operation='RECONCILIATION' and v_error='IDENTITY_LINK_MISSING' then
        v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'classification','NOT_ENROLLED'));
      elsif v_route_operation='RECONCILIATION' and v_error in ('IDENTITY_LINK_AMBIGUOUS','VALIDATION_FAILED') then
        v_outcomes:=v_outcomes||jsonb_build_array(jsonb_build_object('index',v_index,'classification','TERMINAL_CONFLICT','error_code',v_error));
      else
        raise;
      end if;
    end;
    v_index:=v_index+1;
  end loop;

  if v_route_operation='RECONCILIATION' then
    for v_refresh_candidate_id in
      select candidate_id from unnest(v_reconciled_candidates) candidate_id order by candidate_id
    loop
      perform private._candidate_daily_refresh_sync_state_v1(
        v_environment,v_refresh_candidate_id,'MASTER_AVAILABILITY_SHEET',now());
      update private.candidate_daily_sync_state set last_reconciled_at_utc=now(),
        observed_source_revision=v_reconciled_revisions->>v_refresh_candidate_id::text,updated_at_utc=now()
      where environment=v_environment and candidate_id=v_refresh_candidate_id
        and target='MASTER_AVAILABILITY_SHEET';
    end loop;
  end if;

  v_terminal:=jsonb_build_object('batch_receipt_id',v_batch.batch_receipt_id,'outcomes',v_outcomes);
  update private.candidate_daily_batch_receipts set state='COMPLETED',terminal_http_status=200,
    terminal_response_body=v_terminal,terminal_response_sha256=private._candidate_daily_json_sha256_v1(v_terminal),
    completed_at_utc=now(),updated_at_utc=now() where batch_receipt_id=v_batch.batch_receipt_id;
  return v_terminal;
end;
$function$;

revoke all on function public.candidate_daily_reconciliation_apply_atomic_v1(jsonb,uuid,text,jsonb,text) from public;

do $grants$
begin
  if exists(select 1 from pg_roles where rolname='anon') then
    revoke all on function public.candidate_daily_reconciliation_apply_atomic_v1(jsonb,uuid,text,jsonb,text) from anon;
  end if;
  if exists(select 1 from pg_roles where rolname='authenticated') then
    revoke all on function public.candidate_daily_reconciliation_apply_atomic_v1(jsonb,uuid,text,jsonb,text) from authenticated;
  end if;
  if exists(select 1 from pg_roles where rolname='service_role') then
    grant execute on function public.candidate_daily_reconciliation_apply_atomic_v1(jsonb,uuid,text,jsonb,text) to service_role;
  end if;
end;
$grants$;

commit;
