create or replace function public.invoice_unissue_one(
  p_invoice_id uuid,
  p_actor_user_id uuid,
  p_clear_pdf boolean default false
) returns table(status text,cleared_pdf boolean)
language plpgsql
security definer
set search_path to 'public','pg_temp'
as $function$
declare
  v_now timestamptz:=now();
  v_inv public.invoices%rowtype;
  v_historical_version uuid;
  v_document_version_ids uuid[]:=array[]::uuid[];
  v_document_operation_ids uuid[]:=array[]::uuid[];
  v_issue_operation_ids uuid[]:=array[]::uuid[];
  v_delivery_operation_ids uuid[]:=array[]::uuid[];
  v_operation_ids uuid[]:=array[]::uuid[];
  v_role text;
  v_service boolean:=coalesce(auth.role(),'')='service_role';
begin
  if p_invoice_id is null then raise exception 'invoice_id is required'; end if;
  if not v_service and(auth.uid() is null or auth.uid() is distinct from p_actor_user_id) then
    raise exception using errcode='42501',message='Authenticated actor mismatch';
  end if;
  select lower(btrim(coalesce(u.role,''))) into v_role
  from public.tms_users u where u.id=p_actor_user_id and u.is_active;
  if(not found or v_role<>'admin') and not v_service then
    raise exception using errcode='42501',message='Invoice administrator permission required';
  end if;

  perform public._ctms_assert_invoice_can_unissue_v1(p_invoice_id,true,'INVOICE_UNISSUE');
  perform public._ctms_assert_invoice_correction_lines_v1(
    p_invoice_id,p_actor_user_id,true,'INVOICE_UNISSUE');

  select * into v_inv from public.invoices where id=p_invoice_id for update;
  if not found then raise exception 'Invoice not found'; end if;
  if v_inv.type::text='CREDIT_NOTE' then raise exception 'Cannot unissue a CREDIT_NOTE'; end if;
  if v_inv.status::text='PAID' then raise exception 'Cannot unissue a PAID invoice'; end if;
  if v_inv.status::text='DRAFT' then
    status:='DRAFT';cleared_pdf:=false;return next;return;
  end if;
  if v_inv.status::text<>'ISSUED' then
    raise exception 'Only ISSUED invoices can be unissued (current status=%)',v_inv.status::text;
  end if;
  v_historical_version:=v_inv.issued_document_version_id;

  with changed as materialized (
    update public.invoice_document_versions v
    set status='SUPERSEDED',superseded_at_utc=v_now,
      error_json=jsonb_build_object(
        'code','INVOICE_UNISSUED','invoice_id',p_invoice_id)
    where v.entity_type='INVOICE' and v.entity_id=p_invoice_id
      and v.id is distinct from v_historical_version
      and v.status in(
        'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING')
    returning v.id,v.operation_id
  )
  select coalesce(array_agg(id),array[]::uuid[]),
    coalesce(array_agg(distinct operation_id),array[]::uuid[])
  into v_document_version_ids,v_document_operation_ids
  from changed;

  update public.invoice_operation_chunks c
  set status='SUPERSEDED',phase='SUPERSEDED',
    lease_owner=null,lease_token=null,lease_expires_at_utc=null,
    completed_at_utc=v_now,updated_at_utc=v_now,
    error_json=jsonb_build_object(
      'code','INVOICE_UNISSUED','invoice_id',p_invoice_id)
  where c.document_version_id=any(v_document_version_ids)
    and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

  update public.invoice_operations o
  set control_version=o.control_version+1,updated_at_utc=v_now,
    change_seq=nextval('public.invoice_operation_change_seq')
  where o.id=any(v_document_operation_ids)
    and o.operation_type='BUILD_DOCUMENT'
    and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

  with changed as materialized (
    update public.invoice_operation_chunks c
    set status='SUPERSEDED',phase='SUPERSEDED',
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      completed_at_utc=v_now,updated_at_utc=v_now,
      error_json=jsonb_build_object(
        'code','INVOICE_UNISSUED','invoice_id',p_invoice_id)
    where c.chunk_type='ISSUE_INVOICE' and c.entity_type='INVOICE'
      and c.entity_id=p_invoice_id
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning c.operation_id
  )
  select coalesce(array_agg(distinct operation_id),array[]::uuid[])
  into v_issue_operation_ids from changed;

  with changed as materialized (
    update public.invoice_operation_chunks c
    set status='CANCELLED',phase='CANCELLED',
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      completed_at_utc=v_now,updated_at_utc=v_now,
      error_json=jsonb_build_object(
        'code','INVOICE_UNISSUED','invoice_id',p_invoice_id)
    where c.chunk_type='DELIVERY_PREPARE' and c.entity_type='INVOICE'
      and c.entity_id=p_invoice_id
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning c.operation_id
  )
  select coalesce(array_agg(distinct operation_id),array[]::uuid[])
  into v_delivery_operation_ids from changed;

  v_operation_ids:=array(
    select distinct x
    from unnest(coalesce(v_document_operation_ids,array[]::uuid[])
      ||coalesce(v_issue_operation_ids,array[]::uuid[])
      ||coalesce(v_delivery_operation_ids,array[]::uuid[])) x
    where x is not null);

  perform 1 from public.invoice_operations o
  where o.id=any(v_operation_ids) for update;

  perform 1
  from private._invoice_operation_rollup_batch(v_operation_ids,v_now,true);

  update public.mail_outbox m
  set status='FAILED',failed_at=v_now,last_error='CANCELLED_BY_INVOICE_UNISSUE',
    attempt_lease_token=null,attempt_leased_at_utc=null,attempt_lease_expires_at_utc=null
  where m.status='QUEUED'
    and v_historical_version is not null
    and jsonb_typeof(m.attachments)='array'
    and exists(
      select 1 from jsonb_array_elements(m.attachments) descriptor(value)
      where descriptor.value->>'invoice_id'=p_invoice_id::text
        and descriptor.value->>'document_version_id'=v_historical_version::text);

  update public.invoices
  set status='DRAFT',status_date_utc=v_now,issued_at_utc=null,due_at_utc=null,
    on_hold_reason=null,invoice_pdf_r2_key=null,invoice_pdf_generated_at_utc=null,
    issued_document_version_id=null,preview_document_version_id=null,
    active_document_operation_id=null,active_issue_operation_id=null,
    document_revision=document_revision+1,
    document_state='STALE',issue_state='NOT_STARTED',
    header_snapshot_json=coalesce(header_snapshot_json,'{}')||jsonb_build_object(
      'last_unissued_document_version_id',v_historical_version,
      'last_unissued_at_utc',v_now),
    updated_at=v_now
  where id=p_invoice_id;

  perform public._audit_insert('invoice',p_invoice_id::text,'INVOICE_UNISSUED',null,
    jsonb_build_object(
      'clear_pdf_requested_ignored',p_clear_pdf,
      'compatibility_pointer_cleared',true,
      'historical_document_version_id',v_historical_version,
      'historical_object_preserved',true,
      'fenced_operation_ids',to_jsonb(v_operation_ids)),null,p_actor_user_id);

  status:='DRAFT';cleared_pdf:=true;return next;
end;
$function$;

revoke all on function public.invoice_unissue_one(uuid,uuid,boolean) from public,anon;
grant execute on function public.invoice_unissue_one(uuid,uuid,boolean)
  to authenticated,service_role;
