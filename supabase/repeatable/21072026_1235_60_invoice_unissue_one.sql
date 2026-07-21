-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: 94ddb7c35ecd.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
create or replace function public.invoice_unissue_one(
  p_invoice_id uuid,
  p_actor_user_id uuid,
  p_clear_pdf boolean default false
)
returns table (
  status text,
  cleared_pdf boolean
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  perform public._ctms_assert_invoice_can_unissue_v1(
    p_invoice_id,true,'INVOICE_UNISSUE'
  );
  perform public._ctms_assert_invoice_correction_lines_v1(
    p_invoice_id,p_actor_user_id,true,'INVOICE_UNISSUE'
  );

  declare
    v_inv record;
  begin
    select *
    into v_inv
    from public.invoices
    where id = p_invoice_id;

    if not found then
      raise exception 'Invoice not found';
    end if;

    if v_inv.type::text = 'CREDIT_NOTE' then
      raise exception 'Cannot unissue a CREDIT_NOTE';
    end if;

    if v_inv.status::text = 'PAID' then
      raise exception 'Cannot unissue a PAID invoice';
    end if;

    if v_inv.status::text = 'DRAFT' then
      status := 'DRAFT';
      cleared_pdf := false;
      return next;
      return;
    end if;

    if v_inv.status::text <> 'ISSUED' then
      raise exception 'Only ISSUED invoices can be unissued (current status=%)', v_inv.status::text;
    end if;
  end;

  update public.invoices
  set status = 'DRAFT'::public.invoice_status_enum,
      status_date_utc = v_now,
      issued_at_utc = null,
      due_at_utc = null,
      on_hold_reason = null,
      invoice_pdf_r2_key = null,
      invoice_pdf_generated_at_utc = null
  where id = p_invoice_id;

  perform public._audit_insert(
    'invoice',
    p_invoice_id::text,
    'INVOICE_UNISSUED',
    null,
    jsonb_build_object(
      'clear_pdf_requested', p_clear_pdf,
      'clear_pdf_applied', true
    ),
    null,
    p_actor_user_id
  );

  status := 'DRAFT';
  cleared_pdf := true;
  return next;
end;
$$;
