create or replace function public.manual_timesheet_queue_attach_process_atomic(
  p_queue_id uuid,
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_expected_storage_key text,
  p_kind text default 'TIMESHEET'::text,
  p_actor_user_id uuid default null::uuid,
  p_source_json jsonb default '{}'::jsonb,
  p_now_utc timestamptz default now()
) returns jsonb
language plpgsql
security definer
set search_path to 'public','extensions','pg_temp'
as $function$
declare
  v_now timestamptz:=coalesce(p_now_utc,now());
  v_queue public.manual_timesheet_queue%rowtype;
  v_ts public.timesheets%rowtype;
  v_kind text:=upper(btrim(coalesce(p_kind,'TIMESHEET')));
  v_expected_key text:=nullif(regexp_replace(coalesce(btrim(p_expected_storage_key),''),'^/+','','g'),'');
  v_key text; v_evidence_id uuid; v_asset_id uuid; v_operation_id uuid;
  v_document_operation_id uuid; v_document_version_id uuid;
  v_document_revision bigint; v_document_status text; v_document_r2_key text;
  v_document_idempotency text;
  v_display_name text; v_rotation_raw integer; v_rotation integer;
  v_revision text; v_evidence_inserted boolean:=false; v_asset_reused boolean:=false;
  v_asset_state text; v_asset_ready boolean:=false; v_normalised_r2_key text;
  v_asset_error jsonb; v_terminal_asset boolean:=false; v_actor_role text;
  v_state text; v_msg text; v_detail text;
begin
  perform set_config('lock_timeout','2500ms',true);

  if coalesce(auth.role(),'')<>'service_role' then
    if auth.uid() is null or p_actor_user_id is distinct from auth.uid() then
      raise exception using errcode='42501',message='AUTHENTICATED_ACTOR_REQUIRED';
    end if;
  end if;
  if p_actor_user_id is null or not exists(
    select 1 from public.tms_users u where u.id=p_actor_user_id and u.is_active
  ) then
    raise exception using errcode='42501',message='ACTIVE_ACTOR_REQUIRED';
  end if;
  select lower(btrim(coalesce(u.role,''))) into v_actor_role
  from public.tms_users u where u.id=p_actor_user_id and u.is_active;
  if v_actor_role<>'admin' then
    raise exception using errcode='42501',message='TIMESHEET_ADMIN_REQUIRED';
  end if;

  if p_queue_id is null then return jsonb_build_object('ok',false,'error_code','QUEUE_ID_REQUIRED'); end if;
  if p_timesheet_id is null then return jsonb_build_object('ok',false,'error_code','TIMESHEET_ID_REQUIRED'); end if;
  if p_expected_timesheet_id is null then return jsonb_build_object('ok',false,'error_code','EXPECTED_TIMESHEET_ID_REQUIRED'); end if;
  if v_expected_key is null then return jsonb_build_object('ok',false,'error_code','QUEUE_ITEM_STORAGE_REQUIRED'); end if;

  if v_kind in('','TS') then v_kind:='TIMESHEET';
  elsif v_kind in('EXPENSE','EXPENSES') then v_kind:='TRAVEL';
  elsif v_kind in('MILES','MILE') then v_kind:='MILEAGE';
  elsif v_kind='ACCOM' then v_kind:='ACCOMMODATION'; end if;
  if v_kind not in('TIMESHEET','MILEAGE','TRAVEL','ACCOMMODATION','OTHER') then
    return jsonb_build_object('ok',false,'error_code','INVALID_EVIDENCE_KIND','kind',p_kind);
  end if;

  select * into v_ts
  from public.timesheets
  where timesheet_id=p_timesheet_id and is_current
  for update;
  if not found then return jsonb_build_object('ok',false,'error_code','TIMESHEET_NOT_FOUND'); end if;
  if v_ts.timesheet_id is distinct from p_expected_timesheet_id then
    return jsonb_build_object('ok',false,'error_code','TIMESHEET_MOVED',
      'current_timesheet_id',v_ts.timesheet_id,'expected_timesheet_id',p_expected_timesheet_id);
  end if;

  if exists(
    select 1 from public.timesheets_financials tf
    where tf.timesheet_id=v_ts.timesheet_id and tf.is_current
      and(
        tf.locked_by_invoice_id is not null
        or exists(
          select 1 from jsonb_array_elements(
            case when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
              then tf.invoice_breakdown_json->'segments' else '[]'::jsonb end) seg(value)
          where nullif(btrim(seg.value->>'invoice_locked_invoice_id'),'') is not null))
  ) then
    return jsonb_build_object(
      'ok',false,'error_code','TIMESHEET_DOCUMENT_LOCKED');
  end if;

  if lower(coalesce(public._ctms_import_correction_classify_v1(v_ts.timesheet_id)
      ->>'is_import_authoritative_correction','false')) in('true','t','1','yes') then
    return jsonb_build_object(
      'ok',false,'error_code','IMPORT_AUTHORITATIVE_EVIDENCE_READ_ONLY');
  end if;

  if v_kind='TIMESHEET' and exists(
    select 1 from public.v_timesheets_summary_base s
    where s.timesheet_id=v_ts.timesheet_id
      and(coalesce(s.client_no_timesheet_required,false)
        or coalesce(s.client_is_nhsp,false))
  ) then
    return jsonb_build_object(
      'ok',false,'error_code','TIMESHEET_EVIDENCE_NOT_PERMITTED');
  end if;

  -- CloudTMS staff access is represented by an active tms_users record.  The
  -- queue item and current-timesheet locks below are the entity-scoped authority:
  -- callers cannot attach an object to a different or superseded timesheet.
  select * into v_queue
  from public.manual_timesheet_queue
  where id=p_queue_id
  for update;
  if not found then return jsonb_build_object('ok',false,'error_code','QUEUE_ITEM_NOT_AVAILABLE'); end if;
  v_key:=nullif(regexp_replace(coalesce(btrim(v_queue.r2_key),''),'^/+','','g'),'');
  if v_key is null or v_key is distinct from v_expected_key then
    return jsonb_build_object('ok',false,'error_code','QUEUE_ITEM_STORAGE_MISMATCH',
      'expected_storage_key',v_expected_key,'storage_key',v_key);
  end if;
  if upper(coalesce(v_queue.status,''))<>'QUEUED' or v_queue.timesheet_id is not null then
    return jsonb_build_object('ok',false,'error_code','QUEUE_ITEM_NOT_AVAILABLE',
      'status',v_queue.status,'timesheet_id',v_queue.timesheet_id);
  end if;

  if v_kind='TIMESHEET' and exists(
    select 1 from public.timesheet_evidence te
    where te.timesheet_id=v_ts.timesheet_id and upper(te.kind)='TIMESHEET'
      and regexp_replace(te.storage_key,'^/+','','g') is distinct from v_key
  ) then
    return jsonb_build_object('ok',false,'error_code','TIMESHEET_EVIDENCE_ALREADY_EXISTS');
  end if;

  select te.id into v_evidence_id
  from public.timesheet_evidence te
  where te.timesheet_id=v_ts.timesheet_id and upper(te.kind)=v_kind
    and regexp_replace(te.storage_key,'^/+','','g')=v_key
  order by te.created_at desc,te.id desc
  limit 1 for update;

  v_display_name:=coalesce(nullif(btrim(v_queue.original_filename),''),
    reverse(split_part(reverse(v_key),'/',1)));
  v_rotation_raw:=((coalesce(v_queue.last_rotation_deg,0)%360)+360)%360;
  v_rotation:=case when v_rotation_raw>=315 or v_rotation_raw<45 then 0
    when v_rotation_raw<135 then 90 when v_rotation_raw<225 then 180 else 270 end;
  v_revision:=encode(digest(concat_ws('|',v_ts.timesheet_id::text,v_kind,v_key,
    coalesce(v_queue.content_hash,''),v_rotation::text),'sha256'),'hex');

  if v_evidence_id is null then
    insert into public.timesheet_evidence(
      timesheet_id,kind,display_name,storage_key,created_at,created_by,
      source_revision,processing_state
    ) values(
      v_ts.timesheet_id,v_kind,v_display_name,v_key,v_now,p_actor_user_id,
      v_revision,'DISCOVERED'
    ) returning id into v_evidence_id;
    v_evidence_inserted:=true;
  else
    update public.timesheet_evidence
    set source_revision=v_revision
    where id=v_evidence_id and source_revision is distinct from v_revision;
  end if;

  select a.id,a.status,a.operation_id,a.normalised_r2_key,a.error_json
    into v_asset_id,v_asset_state,v_operation_id,v_normalised_r2_key,v_asset_error
  from public.invoice_document_assets a
  where a.source_kind='TIMESHEET_EVIDENCE' and a.source_id=v_evidence_id
    and a.source_revision=v_revision and a.original_r2_key=v_key
  for update;

  if found then
    v_asset_reused:=true;
  else
    insert into public.invoice_document_assets(
      source_kind,source_id,source_revision,original_r2_key,original_filename,
      declared_media_type,original_sha256,orientation_degrees,status,
      created_at_utc,updated_at_utc
    ) values(
      'TIMESHEET_EVIDENCE',v_evidence_id,v_revision,v_key,v_display_name,
      v_queue.mime_type,v_queue.content_hash,v_rotation,'DISCOVERED',v_now,v_now
    ) returning id,status into v_asset_id,v_asset_state;
  end if;
  v_asset_ready:=v_asset_state='READY';
  v_terminal_asset:=v_asset_state in(
    'UNSUPPORTED','CORRUPT','MISSING','FAILED','SUPERSEDED');

  if not v_asset_ready and not v_terminal_asset then
    select o.id into v_operation_id
    from public.invoice_operations o
    where o.operation_type='PREPARE_ASSET'
      and o.entity_type='DOCUMENT_ASSET' and o.entity_id=v_asset_id
      and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
    order by o.created_at_utc desc limit 1 for update;

    if v_operation_id is null
       and v_asset_reused
       and v_asset_state in('INSPECTING','NORMALISING') then
      raise exception using errcode='55000',
        message='ASSET_WORKFLOW_INCONSISTENT';
    end if;

    if v_operation_id is null then
      insert into public.invoice_operations(
        operation_type,entity_type,entity_id,actor_user_id,idempotency_key,status,phase,
        priority,source_revision,input_json,config_json,progress_json,total_units,chunk_count,
        control_version,change_seq,created_at_utc,updated_at_utc
      ) values(
        'PREPARE_ASSET','DOCUMENT_ASSET',v_asset_id,p_actor_user_id,
        encode(digest('PREPARE_ASSET|'||v_asset_id||'|'||v_revision,'sha256'),'hex'),
        'QUEUED','INSPECT',550,v_revision,
        jsonb_build_object('asset_id',v_asset_id,'source_kind','TIMESHEET_EVIDENCE',
          'source_id',v_evidence_id,'source_revision',v_revision,
          'original_r2_key',v_key,'original_filename',v_display_name,
          'declared_media_type',v_queue.mime_type,'rotation_degrees',v_rotation),
        jsonb_build_object('processor_policy',private._invoice_processor_limits()),
        jsonb_build_object('status_message','Asset queued for inspection'),
        1,1,1,nextval('public.invoice_operation_change_seq'),v_now,v_now
      ) returning id into v_operation_id;
    else
      update public.invoice_operations set priority=greatest(priority,550),
        change_seq=nextval('public.invoice_operation_change_seq'),updated_at_utc=v_now
      where id=v_operation_id;
    end if;

    update public.invoice_document_assets
    set operation_id=v_operation_id,updated_at_utc=v_now
    where id=v_asset_id and status<>'READY';

    insert into public.invoice_operation_chunks(
      operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
      document_asset_id,status,priority,run_after_utc,payload_json,
      operation_control_version,created_at_utc,updated_at_utc
    )
    select v_operation_id,'ASSET_INSPECT','INSPECT',
      encode(digest(concat_ws('|','ASSET_INSPECT',v_asset_id::text,v_revision,
        private._invoice_processor_limits()->>'policy_version'),'sha256'),'hex'),
      0,'DOCUMENT_ASSET',v_asset_id,
      v_asset_id,'QUEUED',550,v_now,
      jsonb_build_object('source_kind','TIMESHEET_EVIDENCE',
        'source_id',v_evidence_id,'source_revision',v_revision,'original_r2_key',v_key),
      o.control_version,v_now,v_now
    from public.invoice_operations o
    where o.id=v_operation_id
      and not exists(
        select 1 from public.invoice_operation_chunks ac
        where ac.document_asset_id=v_asset_id and ac.chunk_type in('ASSET_INSPECT','ASSET_NORMALISE')
          and ac.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT')
      )
    on conflict(operation_id,chunk_type,level_no,sequence_no,work_key) do nothing;
  else
    v_operation_id:=null;
  end if;

  update public.timesheet_evidence
  set document_asset_id=v_asset_id,processing_state=v_asset_state,
    processing_error_json=case when v_terminal_asset then v_asset_error else null end
  where id=v_evidence_id;

  if v_kind='TIMESHEET' then
    update public.timesheets
    set manual_document_asset_id=v_asset_id,
      manual_pdf_r2_key=null,
      manual_pdf_rotation_degrees=v_rotation,
      document_state=case when v_terminal_asset then 'FAILED' else 'QUEUED' end,
      last_document_error_json=case when v_terminal_asset then v_asset_error else null end,
      updated_at=v_now
    where timesheet_id=v_ts.timesheet_id and is_current
    returning document_revision into v_document_revision;
  else
    select t.document_revision into v_document_revision
    from public.timesheets t
    where t.timesheet_id=v_ts.timesheet_id and t.is_current;
  end if;

  if not v_terminal_asset then
      select v.id,v.operation_id,v.status,v.r2_key
        into v_document_version_id,v_document_operation_id,
          v_document_status,v_document_r2_key
      from public.invoice_document_versions v
      where v.entity_type='TIMESHEET' and v.entity_id=v_ts.timesheet_id
        and v.purpose='TIMESHEET'
        and v.source_revision=v_document_revision::text
        and v.template_version='timesheet-professional-v1'
        and v.status in(
          'PLANNING','WAITING_FOR_INPUTS','RENDERING',
          'ASSEMBLING','VERIFYING','READY')
      order by(v.status='READY') desc,v.created_at_utc desc,v.id desc
      limit 1;

      if v_document_status='READY' then
        update public.timesheets t
        set current_document_version_id=v_document_version_id,
            manual_pdf_r2_key=v_document_r2_key,
            document_state='READY',
            active_document_operation_id=case
              when t.active_document_operation_id=v_document_operation_id
              then null else t.active_document_operation_id end,
            last_document_error_json=null,updated_at=v_now
        where t.timesheet_id=v_ts.timesheet_id and t.is_current
          and t.document_revision=v_document_revision;
      elsif v_document_version_id is null then
        v_document_idempotency:=encode(digest(concat_ws('|',
          'BUILD_DOCUMENT','TIMESHEET',v_ts.timesheet_id::text,'TIMESHEET',
          v_document_revision::text,'timesheet-professional-v1'),
          'sha256'),'hex');

        insert into public.invoice_operations(
          operation_type,entity_type,entity_id,actor_user_id,idempotency_key,
          status,phase,priority,source_revision,template_version,input_json,
          config_json,progress_json,total_units,chunk_count,control_version,
          change_seq,created_at_utc,updated_at_utc
        ) values(
          'BUILD_DOCUMENT','TIMESHEET',v_ts.timesheet_id,p_actor_user_id,
          v_document_idempotency,'QUEUED','BUILD_MANIFEST',550,
          v_document_revision::text,'timesheet-professional-v1',
          jsonb_build_object(
            'entity_type','TIMESHEET','entity_id',v_ts.timesheet_id,
            'purpose','TIMESHEET','source_revision',v_document_revision,
            'template_version','timesheet-professional-v1'),
          jsonb_build_object('processor_policy',private._invoice_processor_limits()),
          jsonb_build_object('status_message','Timesheet document queued'),
          1,1,1,nextval('public.invoice_operation_change_seq'),v_now,v_now
        )
        on conflict do nothing
        returning id into v_document_operation_id;

        if v_document_operation_id is null then
          select o.id into v_document_operation_id
          from public.invoice_operations o
          where o.idempotency_key=v_document_idempotency
            and o.status in(
              'QUEUED','RUNNING','WAITING','RETRY_WAIT')
          order by o.created_at_utc desc,o.id desc
          limit 1;
        end if;

        insert into public.invoice_document_versions(
          entity_type,entity_id,purpose,operation_id,source_revision,
          template_version,status,snapshot_json,snapshot_hash,
          manifest_json,manifest_hash,created_at_utc
        ) values(
          'TIMESHEET',v_ts.timesheet_id,'TIMESHEET',
          v_document_operation_id,v_document_revision::text,
          'timesheet-professional-v1','PLANNING','{}'::jsonb,
          encode(digest('{}','sha256'),'hex'),'[]'::jsonb,
          encode(digest('[]','sha256'),'hex'),v_now
        )
        on conflict(entity_type,entity_id,purpose,source_revision,template_version)
          where purpose in('DRAFT_PREVIEW','TIMESHEET')
            and status in(
              'PLANNING','WAITING_FOR_INPUTS','RENDERING',
              'ASSEMBLING','VERIFYING','READY')
        do nothing
        returning id into v_document_version_id;

        if v_document_version_id is null then
          select v.id,v.operation_id into
            v_document_version_id,v_document_operation_id
          from public.invoice_document_versions v
          where v.entity_type='TIMESHEET' and v.entity_id=v_ts.timesheet_id
            and v.purpose='TIMESHEET'
            and v.source_revision=v_document_revision::text
            and v.template_version='timesheet-professional-v1'
            and v.status in(
              'PLANNING','WAITING_FOR_INPUTS','RENDERING',
              'ASSEMBLING','VERIFYING','READY')
          order by(v.status='READY') desc,v.created_at_utc desc,v.id desc
          limit 1;
        end if;

        insert into public.invoice_operation_chunks(
          operation_id,chunk_type,phase,work_key,sequence_no,entity_type,entity_id,
          document_version_id,status,priority,run_after_utc,payload_json,
          operation_control_version,created_at_utc,updated_at_utc
        )
        select v_document_operation_id,'DOCUMENT_PLAN','BUILD_MANIFEST',
          encode(digest(concat_ws('|','DOCUMENT_PLAN',v_document_version_id::text,
            v_document_revision::text,'timesheet-professional-v1','1'),
            'sha256'),'hex'),
          0,
          'TIMESHEET',v_ts.timesheet_id,v_document_version_id,'QUEUED',550,
          v_now,jsonb_build_object(
            'purpose','TIMESHEET','source_revision',v_document_revision,
            'template_version','timesheet-professional-v1'),
          o.control_version,v_now,v_now
        from public.invoice_operations o
        where o.id=v_document_operation_id
        on conflict(operation_id,chunk_type,level_no,sequence_no,work_key)
          do nothing;

        update public.timesheets t
        set current_document_version_id=v_document_version_id,
            active_document_operation_id=v_document_operation_id,
            document_state='QUEUED',last_document_error_json=null,
            updated_at=v_now
        where t.timesheet_id=v_ts.timesheet_id and t.is_current
          and t.document_revision=v_document_revision;
      else
        update public.timesheets t
        set current_document_version_id=v_document_version_id,
            active_document_operation_id=v_document_operation_id,
            document_state=case when v_document_status='PLANNING'
              then 'QUEUED' else 'PREPARING' end,
            last_document_error_json=null,updated_at=v_now
        where t.timesheet_id=v_ts.timesheet_id and t.is_current
          and t.document_revision=v_document_revision;
      end if;
  end if;

  update public.manual_timesheet_queue
  set status='ATTACHED',timesheet_id=v_ts.timesheet_id,r2_key=v_key,
    meta_json=coalesce(meta_json,'{}')||jsonb_build_object(
      'attached_kind',v_kind,'attached_to_timesheet_id',v_ts.timesheet_id,
      'attached_at_utc',v_now,'attached_storage_key',v_key,
      'asset_id',v_asset_id,'asset_operation_id',v_operation_id,
      'document_operation_id',v_document_operation_id,
      'source',p_source_json->>'source')
  where id=p_queue_id and status='QUEUED' and timesheet_id is null
    and regexp_replace(r2_key,'^/+','','g')=v_key;
  if not found then raise exception 'QUEUE_ITEM_NOT_AVAILABLE'; end if;

  return jsonb_build_object(
    'ok',true,'success',true,'attached',true,'queue_id',p_queue_id,
    'evidence_id',v_evidence_id,'timesheet_id',v_ts.timesheet_id,'kind',v_kind,
    'storage_key',v_key,'asset_id',v_asset_id,
    'operation_id',v_operation_id,'asset_operation_id',v_operation_id,
    'document_operation_id',v_document_operation_id,
    'document_version_id',v_document_version_id,
    'asset_state',v_asset_state,'processing_state',v_asset_state,
    'asset_reused',v_asset_reused,'asset_ready',v_asset_ready,
    'terminal_asset',v_terminal_asset,
    'terminal_error',case when v_terminal_asset then v_asset_error end,
    'retry_required',v_terminal_asset,
    'backend_should_nudge',
      v_operation_id is not null or v_document_operation_id is not null,
    'timesheet_document_revision',(select document_revision from public.timesheets
      where timesheet_id=v_ts.timesheet_id and is_current),
    'queue_item_consumed',true,
    'changed_domains',jsonb_build_array(
      'timesheet_evidence','manual_timesheet_queue','invoice_document_assets',
      case when v_kind='TIMESHEET' then 'timesheets' end,
      case when v_operation_id is not null then 'invoice_operations' end)
  );
exception when others then
  get stacked diagnostics v_state=returned_sqlstate,v_msg=message_text,v_detail=pg_exception_detail;
  if v_state in('55P03','57014') then
    return jsonb_build_object('ok',false,'success',false,'error_code','LOCK_TIMEOUT',
      'sqlstate',v_state,'detail',v_detail);
  end if;
  return jsonb_build_object('ok',false,'success',false,
    'error_code',coalesce(nullif(v_msg,''),v_state),
    'message',coalesce(nullif(v_msg,''),'Attach failed'),
    'sqlstate',v_state,'detail',v_detail,'queue_id',p_queue_id,'timesheet_id',p_timesheet_id);
end;
$function$;

revoke all on function public.manual_timesheet_queue_attach_process_atomic(
  uuid,uuid,uuid,text,text,uuid,jsonb,timestamptz) from public,anon;
grant execute on function public.manual_timesheet_queue_attach_process_atomic(
  uuid,uuid,uuid,text,text,uuid,jsonb,timestamptz) to authenticated,service_role;
