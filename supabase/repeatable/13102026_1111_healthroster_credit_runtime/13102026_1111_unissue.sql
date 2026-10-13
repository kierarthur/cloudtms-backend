create or replace function public._ctms_assert_invoice_can_unissue_v1(
  p_invoice_id uuid,
  p_lock_row boolean default true,
  p_context text default 'IMPORT_CORRECTION'
)
returns void
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $function$
declare v_invoice public.invoices%rowtype; v_credit_count integer;
begin
  if p_lock_row then select * into v_invoice from public.invoices where id=p_invoice_id for update;
  else select * into v_invoice from public.invoices where id=p_invoice_id; end if;
  if not found then raise exception 'INVOICE_NOT_FOUND' using errcode='P0001'; end if;
  -- Scoped credit is downstream authority even for an ordinary original and while its issue/document is pending.
  if exists(select 1 from public.invoices cn
    where cn.original_invoice_id=p_invoice_id and cn.type='CREDIT_NOTE'
      and cn.status in('DRAFT','ISSUED','PAID') and cn.header_snapshot_json?'credit_scope') then
    raise exception 'INVOICE_UNISSUE_BLOCKED_BY_DOWNSTREAM_AUTHORITY'
      using errcode='P0001',detail=jsonb_build_object('context',p_context,'invoice_id',p_invoice_id)::text;
  end if;
  if not exists (
    select 1 from public.invoice_lines il
    where il.invoice_id=p_invoice_id and il.timesheet_id is not null
      and coalesce((public._ctms_import_correction_classify_v1(il.timesheet_id)
        ->>'is_import_authoritative_correction')::boolean,false)
  ) then
    return;
  end if;
  select count(*)::integer into v_credit_count from public.invoices i
  where i.original_invoice_id=p_invoice_id and upper(coalesce(i.type::text,''))='CREDIT_NOTE';
  if v_invoice.paid_at_utc is not null or upper(coalesce(v_invoice.status::text,''))='PAID'
     or v_invoice.credit_note_created_at_utc is not null or v_credit_count>0 then
    raise exception 'INVOICE_UNISSUE_BLOCKED_BY_DOWNSTREAM_AUTHORITY'
      using errcode='P0001',detail=jsonb_build_object('context',p_context,'invoice_id',p_invoice_id)::text;
  end if;
end;
$function$;
