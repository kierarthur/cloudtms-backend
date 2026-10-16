create or replace function private.invoice_credit_scoped_advance_v1(p_claim jsonb,p_now_utc timestamptz)
returns jsonb language plpgsql security definer
set search_path to 'pg_catalog','public','private','extensions','pg_temp'
as $function$
declare
  v_chunk public.invoice_operation_chunks%rowtype; v_operation public.invoice_operations%rowtype;
  v_original public.invoices%rowtype; v_scope jsonb; v_id uuid; v_r uuid; v_x uuid;
  v_now timestamptz:=coalesce(p_now_utc,now()); v_blocker text; v_phase text:=p_claim->>'phase';
begin
  select c.* into strict v_chunk from public.invoice_operation_chunks c where c.id=(p_claim->>'chunk_id')::uuid;
  select o.* into strict v_operation from public.invoice_operations o where o.id=v_chunk.operation_id;
  if v_chunk.payload_json->>'command_type'<>'GENERATE_CREDIT_NOTE'
    or jsonb_typeof(v_chunk.payload_json->'credit_scope')<>'object'
    or not pg_input_is_valid(v_chunk.payload_json#>>'{credit_scope,reversal_timesheet_id}','uuid')
    or v_phase not in('VALIDATE_SOURCES','PLAN','COMMIT') then
    raise exception 'CREDIT_SCOPE_CLAIM_INVALID' using errcode='22023';
  end if;
  v_r:=(v_chunk.payload_json#>>'{credit_scope,reversal_timesheet_id}')::uuid;
  v_id:=case when pg_input_is_valid(v_chunk.payload_json#>>'{plan,planned_invoice_id}','uuid')
    then(v_chunk.payload_json#>>'{plan,planned_invoice_id}')::uuid end;
  v_scope:=private.invoice_credit_scope_context_v1(v_chunk.entity_id,v_r,v_chunk.operation_id,v_id,v_phase='COMMIT');
  v_blocker:=case when (v_scope->>'valid')::boolean is distinct from true then v_scope->>'blocker' end;
  if v_phase='COMMIT' then
    perform 1 from public.tms_users u where u.id=v_operation.actor_user_id for share;
    if v_blocker is null then
      select validation.blocker_code into v_blocker from private._invoice_correction_validate_batch(
        jsonb_build_array(jsonb_build_object('request_key','credit-source:'||v_chunk.entity_id,
          'scope_key',v_chunk.entity_id,'invoice_id',v_chunk.entity_id,'validation_purpose','CREDIT_SOURCE',
          'credit_scope',jsonb_build_object('reversal_timesheet_id',v_r),'request_id',v_scope->>'request_id',
          'target_revision',(select request.target_revision from private.timesheet_correction_requests request
            where request.id=(v_scope->>'request_id')::uuid),'operation_id',v_chunk.operation_id,'planned_credit_note_id',v_id)),
        (v_now at time zone 'Europe/London')::date) validation
        where validation.valid is distinct from true order by validation.blocker_code limit 1;
    end if;
  end if;
  if not exists(select 1 from public.tms_users u where u.id=v_operation.actor_user_id
      and coalesce(u.is_active,false) and upper(u.role::text)='ADMIN') then
    v_blocker:='CREDIT_ACTOR_NOT_ALLOWED';
  end if;
  if v_blocker is not null then
    update public.invoice_operation_chunks c set status=case when v_phase='COMMIT' then 'SUPERSEDED' else 'BLOCKED' end,
      phase=case when v_phase='COMMIT' then 'SUPERSEDED' else 'BLOCKED' end,
      error_json=jsonb_build_object('code',v_blocker,'invoice_id',v_chunk.entity_id,'reversal_timesheet_id',v_r),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,updated_at_utc=v_now where c.id=v_chunk.id;
  elsif v_phase='VALIDATE_SOURCES' then
    update public.invoice_operation_chunks c set status='QUEUED',phase='PLAN',
      payload_json=c.payload_json||jsonb_build_object('source_revision',v_scope->>'revision',
        'source_invoice_id',c.entity_id,'resume_invoice_id',v_scope->'resume_invoice_id'),
      error_json=null,lease_owner=null,lease_token=null,lease_expires_at_utc=null,updated_at_utc=v_now where c.id=v_chunk.id;
  elsif v_phase='PLAN' then
    v_id:=coalesce(v_id,nullif(v_scope->>'resume_invoice_id','')::uuid,gen_random_uuid());
    update public.invoice_operation_chunks c set status='QUEUED',phase='COMMIT',
      payload_json=c.payload_json||jsonb_build_object('plan',jsonb_build_object('planned_invoice_id',v_id,
        'intended_invoice_count',1,'source_count',v_scope->'source_count','source_revision',v_scope->>'revision',
        'expected_charge_ex_vat',v_scope->'subtotal_ex_vat','expected_pay_ex_vat',v_scope->'total_pay_ex_vat',
        'settings_snapshot',v_scope,'settings_hash',encode(digest(v_scope::text,'sha256'),'hex'))),
      error_json=null,lease_owner=null,lease_token=null,lease_expires_at_utc=null,updated_at_utc=v_now where c.id=v_chunk.id;
  elsif nullif(v_scope->>'resume_invoice_id','') is not null then
    -- Own result was validated against exact creating operation, persisted
    -- membership, negative money and original-line identity by resume helper.
    update public.invoice_operation_chunks c set status='QUEUED',phase='QUEUE_DOCUMENT',
      result_json=jsonb_build_object('invoice_ids',jsonb_build_array(v_id),'credit_note_id',v_id,
        'source_invoice_id',c.entity_id,'replayed',true),error_json=null,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,updated_at_utc=v_now where c.id=v_chunk.id;
  elsif v_id is null or v_chunk.payload_json->>'source_revision' is distinct from v_scope->>'revision'
    or v_chunk.payload_json#>>'{plan,source_revision}' is distinct from v_scope->>'revision' then
    update public.invoice_operation_chunks c set status='SUPERSEDED',phase='SUPERSEDED',
      error_json=jsonb_build_object('code','CREDIT_SOURCE_CHANGED_BEFORE_COMMIT'),
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,updated_at_utc=v_now where c.id=v_chunk.id;
  else
    v_x:=(v_scope->>'reversed_timesheet_id')::uuid;
    select i.* into strict v_original from public.invoices i where i.id=v_chunk.entity_id;
    begin
      insert into public.invoices(id,type,client_id,status,status_date_utc,subtotal_ex_vat,vat_amount,total_inc_vat,
        original_invoice_id,notes,header_snapshot_json,document_revision,document_state,created_at,updated_at)
      values(v_id,'CREDIT_NOTE',v_original.client_id,'DRAFT',v_now,(v_scope->>'subtotal_ex_vat')::numeric,
        (v_scope->>'vat_amount')::numeric,(v_scope->>'total_inc_vat')::numeric,v_original.id,
        v_chunk.payload_json->>'credit_reason',v_original.header_snapshot_json||jsonb_build_object(
          'credit_source_invoice_id',v_original.id,'credit_reason',v_chunk.payload_json->>'credit_reason',
          'credit_operation_id',v_chunk.operation_id,'credit_scope',v_scope||jsonb_build_object(
            'creating_operation_id',v_chunk.operation_id)),1,'STALE',v_now,v_now);
      insert into public.invoice_lines(invoice_id,timesheet_id,booking_id,description,
        hours_day,hours_night,hours_sat,hours_sun,hours_bh,pay_day,pay_night,pay_sat,pay_sun,pay_bh,
        charge_day,charge_night,charge_sat,charge_sun,charge_bh,total_pay_ex_vat,total_charge_ex_vat,
        margin_ex_vat,vat_rate_pct,vat_amount,total_inc_vat,paper_ts_r2_key,meta_json,source_key)
      select v_id,l.timesheet_id,l.booking_id,'CREDIT NOTE – '||coalesce(l.description,''),
        l.hours_day,l.hours_night,l.hours_sat,l.hours_sun,l.hours_bh,l.pay_day,l.pay_night,l.pay_sat,l.pay_sun,l.pay_bh,
        l.charge_day,l.charge_night,l.charge_sat,l.charge_sun,l.charge_bh,-l.total_pay_ex_vat,-l.total_charge_ex_vat,
        -l.margin_ex_vat,l.vat_rate_pct,-l.vat_amount,-l.total_inc_vat,l.paper_ts_r2_key,
        coalesce(l.meta_json,'{}'::jsonb)||jsonb_build_object('credit_note',true,'original_invoice_id',v_original.id,
          'original_invoice_line_id',l.id,'credit_of_line_id',l.id,'reversal_timesheet_id',v_r,
          'line_type','CREDIT_'||coalesce(l.meta_json->>'line_type','LINE')),
        'CN:'||v_id||':LINE:'||l.id
      from public.invoice_lines l where l.invoice_id=v_original.id and l.timesheet_id=v_x;
      update public.timesheets_financials tf set locked_by_invoice_id=v_id,locked_at_utc=v_now,
        invoice_breakdown_json=case when jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
          then jsonb_set(tf.invoice_breakdown_json,'{segments}',coalesce((select jsonb_agg(s.value||jsonb_build_object(
            'invoice_locked_invoice_id',v_id,'invoice_locked_at_utc',v_now) order by s.ordinality)
            from jsonb_array_elements(tf.invoice_breakdown_json->'segments') with ordinality s(value,ordinality)),'[]'::jsonb),true)
          else tf.invoice_breakdown_json end,updated_at=v_now
        where tf.timesheet_id=v_r and tf.is_current and tf.locked_by_invoice_id is null;
      if not found then raise exception 'CREDIT_SCOPE_ALREADY_CREDITED' using errcode='P0001'; end if;
      update public.contract_weeks w set status='INVOICED',updated_at=v_now where w.timesheet_id=v_r;
      if (v_scope->>'attach_hr')::boolean then
        insert into public.invoice_hr_source_rows(invoice_id,source_system,import_id,header_columns,rows_json,header_rows,member_rows_json)
        select v_id,s.source_system,s.import_id,s.header_columns,s.member_rows_json->v_x::text,s.header_rows,
          jsonb_build_object(v_x::text,s.member_rows_json->v_x::text)
        from public.invoice_hr_source_rows s where s.invoice_id=v_original.id and upper(s.source_system)='HEALTHROSTER'
          and jsonb_typeof(s.member_rows_json->v_x::text)='array' and jsonb_array_length(s.member_rows_json->v_x::text)>0;
      end if;
      insert into public.audit_events(ts_utc,actor_user_id,object_type,object_id_text,action,after_json,reason)
      values(v_now,v_operation.actor_user_id,'invoices',v_id::text,'CREDIT_NOTE_CREATED',
        jsonb_build_object('credit_note_id',v_id,'original_invoice_id',v_original.id,'reversal_timesheet_id',v_r,
          'reversed_timesheet_id',v_x,'creating_operation_id',v_chunk.operation_id,
          'subtotal_ex_vat',v_scope->'subtotal_ex_vat','vat_amount',v_scope->'vat_amount','total_inc_vat',v_scope->'total_inc_vat'),
        'HEALTHROSTER_SCOPED_CREDIT');
      -- I1, X/T0 and every unrelated TF/segment/evidence row are deliberately
      -- not financial writes in this arm. All native triggers remain enabled.
      update public.invoice_operation_chunks c set status='QUEUED',phase='QUEUE_DOCUMENT',
        result_json=jsonb_build_object('invoice_ids',jsonb_build_array(v_id),'credit_note_id',v_id,'source_invoice_id',v_original.id),
        error_json=null,lease_owner=null,lease_token=null,lease_expires_at_utc=null,updated_at_utc=v_now where c.id=v_chunk.id;
    exception when others then
      if sqlerrm<>'CREDIT_SCOPE_ALREADY_CREDITED' then raise; end if;
      -- The subtransaction rolled the header/lines back before the loser receipt.
      update public.invoice_operation_chunks c set status='SUPERSEDED',phase='SUPERSEDED',
        error_json=jsonb_build_object('code','CREDIT_SCOPE_ALREADY_CREDITED'),
        lease_owner=null,lease_token=null,lease_expires_at_utc=null,updated_at_utc=v_now where c.id=v_chunk.id;
    end;
  end if;
  select c.* into strict v_chunk from public.invoice_operation_chunks c where c.id=v_chunk.id;
  return jsonb_build_object('chunk_id',v_chunk.id,'status',v_chunk.status,'phase',v_chunk.phase,
    'result',v_chunk.result_json,'error',v_chunk.error_json);
end;
$function$;
