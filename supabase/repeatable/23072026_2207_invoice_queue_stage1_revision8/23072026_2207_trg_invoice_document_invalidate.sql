create or replace function public.trg_invoice_document_invalidate()
returns trigger
language plpgsql
security definer
set search_path to 'public','pg_temp'
as $function$
declare
  v_invoice_ids uuid[]:=array[]::uuid[];
  v_version_ids uuid[]:=array[]::uuid[];
  v_document_operation_ids uuid[]:=array[]::uuid[];
  v_issue_operation_ids uuid[]:=array[]::uuid[];
  v_delivery_operation_ids uuid[]:=array[]::uuid[];
  v_operation_ids uuid[]:=array[]::uuid[];
begin
  if tg_table_name='invoices' and tg_op='UPDATE' then
    select coalesce(array_agg(distinct n.id),array[]::uuid[]) into v_invoice_ids
    from new_rows n join old_rows o on o.id=n.id
    where n.status='DRAFT' and o.status='DRAFT' and(
       n.client_id is distinct from o.client_id
       or n.invoice_no is distinct from o.invoice_no
       or n.type is distinct from o.type
       or n.original_invoice_id is distinct from o.original_invoice_id
       or n.subtotal_ex_vat is distinct from o.subtotal_ex_vat
       or n.vat_amount is distinct from o.vat_amount
       or n.total_inc_vat is distinct from o.total_inc_vat
       or n.due_at_utc is distinct from o.due_at_utc
       or n.notes is distinct from o.notes
       or n.do_not_send is distinct from o.do_not_send
       or n.header_snapshot_json is distinct from o.header_snapshot_json);
  elsif tg_table_name='invoice_lines' and tg_op='INSERT' then
    select coalesce(array_agg(distinct invoice_id),array[]::uuid[]) into v_invoice_ids from new_rows;
  elsif tg_table_name='invoice_lines' and tg_op='DELETE' then
    select coalesce(array_agg(distinct invoice_id),array[]::uuid[]) into v_invoice_ids from old_rows;
  elsif tg_table_name='invoice_lines' and tg_op='UPDATE' then
    select coalesce(array_agg(distinct invoice_id),array[]::uuid[]) into v_invoice_ids
    from (
      select n.invoice_id from new_rows n join old_rows o on o.id=n.id
      where n.invoice_id is distinct from o.invoice_id
         or n.timesheet_id is distinct from o.timesheet_id
         or n.booking_id is distinct from o.booking_id
         or n.source_key is distinct from o.source_key
         or n.description is distinct from o.description
         or n.hours_day is distinct from o.hours_day
         or n.hours_night is distinct from o.hours_night
         or n.hours_sat is distinct from o.hours_sat
         or n.hours_sun is distinct from o.hours_sun
         or n.hours_bh is distinct from o.hours_bh
         or n.pay_day is distinct from o.pay_day
         or n.pay_night is distinct from o.pay_night
         or n.pay_sat is distinct from o.pay_sat
         or n.pay_sun is distinct from o.pay_sun
         or n.pay_bh is distinct from o.pay_bh
         or n.charge_day is distinct from o.charge_day
         or n.charge_night is distinct from o.charge_night
         or n.charge_sat is distinct from o.charge_sat
         or n.charge_sun is distinct from o.charge_sun
         or n.charge_bh is distinct from o.charge_bh
         or n.total_pay_ex_vat is distinct from o.total_pay_ex_vat
         or n.total_charge_ex_vat is distinct from o.total_charge_ex_vat
         or n.vat_rate_pct is distinct from o.vat_rate_pct
         or n.vat_amount is distinct from o.vat_amount
         or n.total_inc_vat is distinct from o.total_inc_vat
         or n.margin_ex_vat is distinct from o.margin_ex_vat
         or n.meta_json is distinct from o.meta_json
      union
      select o.invoice_id from old_rows o join new_rows n on n.id=o.id
      where n.invoice_id is distinct from o.invoice_id
         or n.timesheet_id is distinct from o.timesheet_id
         or n.booking_id is distinct from o.booking_id
         or n.source_key is distinct from o.source_key
         or n.description is distinct from o.description
         or n.hours_day is distinct from o.hours_day
         or n.hours_night is distinct from o.hours_night
         or n.hours_sat is distinct from o.hours_sat
         or n.hours_sun is distinct from o.hours_sun
         or n.hours_bh is distinct from o.hours_bh
         or n.pay_day is distinct from o.pay_day
         or n.pay_night is distinct from o.pay_night
         or n.pay_sat is distinct from o.pay_sat
         or n.pay_sun is distinct from o.pay_sun
         or n.pay_bh is distinct from o.pay_bh
         or n.charge_day is distinct from o.charge_day
         or n.charge_night is distinct from o.charge_night
         or n.charge_sat is distinct from o.charge_sat
         or n.charge_sun is distinct from o.charge_sun
         or n.charge_bh is distinct from o.charge_bh
         or n.total_pay_ex_vat is distinct from o.total_pay_ex_vat
         or n.total_charge_ex_vat is distinct from o.total_charge_ex_vat
         or n.vat_rate_pct is distinct from o.vat_rate_pct
         or n.vat_amount is distinct from o.vat_amount
         or n.total_inc_vat is distinct from o.total_inc_vat
         or n.margin_ex_vat is distinct from o.margin_ex_vat
         or n.meta_json is distinct from o.meta_json
    ) s;
  elsif tg_table_name='invoice_hr_source_rows' and tg_op='INSERT' then
    select coalesce(array_agg(distinct invoice_id),array[]::uuid[]) into v_invoice_ids from new_rows;
  elsif tg_table_name='invoice_hr_source_rows' and tg_op='DELETE' then
    select coalesce(array_agg(distinct invoice_id),array[]::uuid[]) into v_invoice_ids from old_rows;
  elsif tg_table_name='invoice_hr_source_rows' and tg_op='UPDATE' then
    select coalesce(array_agg(distinct coalesce(n.invoice_id,o.invoice_id)),array[]::uuid[])
      into v_invoice_ids
    from new_rows n full join old_rows o
      on n.invoice_id=o.invoice_id
     and n.source_system=o.source_system
     and n.import_id is not distinct from o.import_id
    where n.invoice_id is null or o.invoice_id is null
       or n.header_rows is distinct from o.header_rows
       or n.header_columns is distinct from o.header_columns
       or n.rows_json is distinct from o.rows_json;
  end if;

  if cardinality(v_invoice_ids)=0 then return null; end if;

  select coalesce(array_agg(i.id),array[]::uuid[]) into v_invoice_ids
  from public.invoices i
  where i.id=any(v_invoice_ids) and i.status='DRAFT';
  if cardinality(v_invoice_ids)=0 then return null; end if;

  update public.invoices i set document_revision=i.document_revision+1,
    document_state='STALE',preview_document_version_id=null,active_document_operation_id=null,
    invoice_pdf_r2_key=null,invoice_pdf_generated_at_utc=null,
    issue_state=case when i.issue_state in ('VALIDATING','PREPARING_DOCUMENT','READY_TO_FINALISE')
      then 'SUPERSEDED' else i.issue_state end,last_document_error_json=null
  where i.id=any(v_invoice_ids) and i.status='DRAFT';

  with changed as materialized (
    update public.invoice_document_versions v
    set status='SUPERSEDED',superseded_at_utc=now(),
      error_json=jsonb_build_object(
        'code','INVOICE_SOURCE_CHANGED','invoice_id',v.entity_id)
    where v.entity_type='INVOICE' and v.entity_id=any(v_invoice_ids)
      and v.purpose in('DRAFT_PREVIEW','FINAL_ISSUE')
      and v.status in(
        'PLANNING','WAITING_FOR_INPUTS','RENDERING','ASSEMBLING','VERIFYING')
    returning v.id,v.operation_id
  )
  select coalesce(array_agg(id),array[]::uuid[]),
    coalesce(array_agg(distinct operation_id),array[]::uuid[])
  into v_version_ids,v_document_operation_ids from changed;

  with changed as materialized (
    update public.invoice_operation_chunks c
    set status='SUPERSEDED',phase='SUPERSEDED',
      replacement_required=false,replaced_by_chunk_id=null,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      completed_at_utc=now(),updated_at_utc=now(),
      error_json=jsonb_build_object(
        'code','INVOICE_SOURCE_CHANGED','invoice_id',c.entity_id)
    where c.chunk_type='ISSUE_INVOICE' and c.entity_type='INVOICE'
      and c.entity_id=any(v_invoice_ids)
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning c.operation_id
  )
  select coalesce(array_agg(distinct operation_id),array[]::uuid[])
  into v_issue_operation_ids from changed;

  with changed as materialized (
    update public.invoice_operation_chunks c
    set status='SUPERSEDED',phase='SUPERSEDED',
      replacement_required=false,replaced_by_chunk_id=null,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,
      completed_at_utc=now(),updated_at_utc=now(),
      error_json=jsonb_build_object(
        'code','INVOICE_SOURCE_CHANGED','invoice_id',c.entity_id)
    where c.chunk_type='DELIVERY_PREPARE' and c.entity_type='INVOICE'
      and c.entity_id=any(v_invoice_ids)
      and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
    returning c.operation_id
  )
  select coalesce(array_agg(distinct operation_id),array[]::uuid[])
  into v_delivery_operation_ids from changed;

  update public.invoice_operation_chunks c
  set status='SUPERSEDED',phase='SUPERSEDED',
    replacement_required=false,replaced_by_chunk_id=null,
    lease_owner=null,lease_token=null,lease_expires_at_utc=null,
    completed_at_utc=now(),updated_at_utc=now(),
    error_json=jsonb_build_object(
      'code','INVOICE_SOURCE_CHANGED','document_version_id',c.document_version_id)
  where c.document_version_id=any(v_version_ids)
    and c.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

  update public.invoice_operations o
  set control_version=o.control_version+1,updated_at_utc=now(),
    change_seq=nextval('public.invoice_operation_change_seq')
  where o.id=any(v_document_operation_ids)
    and o.operation_type='BUILD_DOCUMENT'
    and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');

  v_operation_ids:=array(
    select distinct x
    from unnest(coalesce(v_document_operation_ids,array[]::uuid[])
      ||coalesce(v_issue_operation_ids,array[]::uuid[])
      ||coalesce(v_delivery_operation_ids,array[]::uuid[])) x
    where x is not null);

  update public.invoices i
  set active_issue_operation_id=null
  where i.id=any(v_invoice_ids)
    and i.active_issue_operation_id=any(v_operation_ids);

  -- A statement trigger must remain bounded to the directly affected rows.  It
  -- marks their roots dirty; the normal worker/reconciliation path performs
  -- descendant aggregation outside the write-trigger call stack.
  update public.invoice_operations o
  set progress_json=jsonb_set(coalesce(o.progress_json,'{}'::jsonb),
        '{rollup_required}','true'::jsonb,true),
    updated_at_utc=now(),
    change_seq=nextval('public.invoice_operation_change_seq')
  where o.id=any(v_operation_ids)
    and o.status in('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED');
  return null;
end;
$function$;

revoke all on function public.trg_invoice_document_invalidate() from public,anon,authenticated;
