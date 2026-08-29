-- CloudTMS reviewed direct replacement; review artifact only, not installed.
-- Exact TEST baseline body MD5 prefix: f3458fd806e3.
-- Ordinary and non-import-authoritative branches remain on the installed implementation.
create or replace function public.invoice_remove_nhsp_shifts(
  p_invoice_id uuid,
  p_shift_ids uuid[],
  p_actor_user_id uuid
)
returns table (
  invoice_id uuid,
  subtotal_ex_vat numeric,
  vat_amount numeric,
  total_inc_vat numeric
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_now timestamptz := now();
  v_shift_ids uuid[];

  -- prev/new totals
  v_prev_ex numeric := 0;
  v_prev_vat numeric := 0;
  v_prev_inc numeric := 0;
  v_new_ex numeric := 0;
  v_new_vat numeric := 0;
  v_new_inc numeric := 0;

  v_delta_ex numeric := 0;
  v_delta_vat numeric := 0;
  v_delta_inc numeric := 0;

  v_invoice_no text := null;
  v_prev_status text := null;
  v_new_status text := null;

  -- removed-lines detail
  v_removed_ts_ids uuid[] := null;
  v_removed_source_keys text[] := null;
  v_removed_line_count int := 0;
  v_removed_ex numeric := 0;
  v_removed_vat numeric := 0;
  v_removed_inc numeric := 0;

  v_pdf_jobs_enqueued int := 0;
begin
  if p_invoice_id is null then
    raise exception 'invoice_id is required';
  end if;

  if p_shift_ids is null or coalesce(array_length(p_shift_ids,1),0) = 0 then
    raise exception 'shift_ids[] required';
  end if;

  v_shift_ids := (select array_agg(distinct x) from unnest(p_shift_ids) x where x is not null);

  perform public._ctms_assert_invoice_mutable_draft_v1(
    p_invoice_id,'INVOICE_REMOVE_NHSP_SHIFTS',true
  );
  if exists (
    select 1 from public.invoice_lines il
    where il.invoice_id=p_invoice_id and il.timesheet_id is not null
      and (il.meta_json->>'nhsp_shift_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and (il.meta_json->>'nhsp_shift_id')::uuid=any(v_shift_ids)
      and coalesce((public._ctms_import_correction_classify_v1(il.timesheet_id)
        ->>'is_import_authoritative_correction')::boolean,false)
  ) then
    raise exception 'IMPORT_CORRECTION_INVOICE_SHIFT_REMOVAL_FORBIDDEN'
      using errcode='P0001',detail=jsonb_build_object('invoice_id',p_invoice_id)::text;
  end if;

  -- Capture invoice BEFORE
  select
    i.invoice_no,
    i.status::text,
    coalesce(i.subtotal_ex_vat,0)::numeric,
    coalesce(i.vat_amount,0)::numeric,
    coalesce(i.total_inc_vat,0)::numeric
  into
    v_invoice_no,
    v_prev_status,
    v_prev_ex,
    v_prev_vat,
    v_prev_inc
  from public.invoices i
  where i.id = p_invoice_id
  limit 1;

  -- Identify lines that will be removed (for audit detail)
  with to_remove as (
    select
      l.timesheet_id,
      l.source_key,
      coalesce(l.total_charge_ex_vat,0)::numeric as ex,
      coalesce(l.vat_amount,0)::numeric as vat,
      coalesce(l.total_inc_vat,0)::numeric as inc
    from public.invoice_lines l
    where l.invoice_id = p_invoice_id
      and (l.meta_json ? 'nhsp_shift_id')
      and (l.meta_json->>'nhsp_shift_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and (l.meta_json->>'nhsp_shift_id')::uuid = any(v_shift_ids)
  )
  select
    array_agg(distinct timesheet_id),
    array_agg(distinct source_key),
    count(*)::int,
    coalesce(sum(ex),0)::numeric,
    coalesce(sum(vat),0)::numeric,
    coalesce(sum(inc),0)::numeric
  into
    v_removed_ts_ids,
    v_removed_source_keys,
    v_removed_line_count,
    v_removed_ex,
    v_removed_vat,
    v_removed_inc
  from to_remove;

  -- 1) Unlink shifts (only those currently linked to this invoice)
  update public.nhsp_shifts s
  set invoice_status = 'PENDING',
      invoice_id = null,
      updated_at = v_now
  where s.id = any(v_shift_ids)
    and s.invoice_id = p_invoice_id;

  -- 2) Delete invoice lines referencing these shifts
  delete from public.invoice_lines l
  where l.invoice_id = p_invoice_id
    and (l.meta_json ? 'nhsp_shift_id')
    and (l.meta_json->>'nhsp_shift_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    and (l.meta_json->>'nhsp_shift_id')::uuid = any(v_shift_ids);

  -- 3) Recompute totals from remaining lines (also clears invoice_pdf_r2_key)
  perform public.invoice_recompute_totals(p_invoice_id);

  -- 3.1) Invalidate cached render artifacts explicitly (policy: draft/on-hold must regen bundle/attachments)
  update public.invoices i0
  set
    invoice_pdf_generated_at_utc = null,
    invoice_render_manifest = null,
    paper_ts_r2_manifest = null,
    updated_at = v_now
  where i0.id = p_invoice_id;

  -- 3.2) Enqueue FORCE_REGEN invoice PDF bundle job (idempotent)
  v_pdf_jobs_enqueued := public.invpdf_enqueue_one(p_invoice_id, true);

  -- 4) Unlock TSFIN if a timesheet now has no remaining lines on this invoice
  update public.timesheets_financials tf
  set locked_by_invoice_id = null,
      updated_at = v_now
  where tf.is_current = true
    and tf.locked_by_invoice_id = p_invoice_id
    and tf.timesheet_id is not null
    and not exists (
      select 1
      from public.invoice_lines l2
      where l2.invoice_id = p_invoice_id
        and l2.timesheet_id = tf.timesheet_id
    );

  -- Capture invoice AFTER + compute delta
  select
    i.status::text,
    coalesce(i.subtotal_ex_vat,0)::numeric,
    coalesce(i.vat_amount,0)::numeric,
    coalesce(i.total_inc_vat,0)::numeric
  into
    v_new_status,
    v_new_ex,
    v_new_vat,
    v_new_inc
  from public.invoices i
  where i.id = p_invoice_id
  limit 1;

  v_delta_ex  := public._inv_round2(v_new_ex  - v_prev_ex);
  v_delta_vat := public._inv_round2(v_new_vat - v_prev_vat);
  v_delta_inc := public._inv_round2(v_new_inc - v_prev_inc);

  -- Existing audit (kept), now with totals + timesheets + delta
  perform public._audit_insert(
    'invoice',
    p_invoice_id::text,
    'NHSP_INVOICE_SHIFT_REMOVED',
    jsonb_build_object(
      'invoice_no', v_invoice_no,
      'status', v_prev_status,
      'subtotal_ex_vat', public._inv_round2(v_prev_ex),
      'vat_amount', public._inv_round2(v_prev_vat),
      'total_inc_vat', public._inv_round2(v_prev_inc)
    ),
    jsonb_build_object(
      'invoice_id', p_invoice_id::text,
      'invoice_no', v_invoice_no,
      'shift_ids', to_jsonb(v_shift_ids),

      'removed_line_count', coalesce(v_removed_line_count,0),
      'removed_timesheet_ids', to_jsonb(coalesce(v_removed_ts_ids, array[]::uuid[])),
      'removed_source_keys', to_jsonb(coalesce(v_removed_source_keys, array[]::text[])),

      'removed_subtotal_ex_vat', public._inv_round2(v_removed_ex),
      'removed_vat_amount', public._inv_round2(v_removed_vat),
      'removed_total_inc_vat', public._inv_round2(v_removed_inc),

      'invoice_status_before', v_prev_status,
      'invoice_status_after', v_new_status,

      'prev_subtotal_ex_vat', public._inv_round2(v_prev_ex),
      'prev_vat_amount', public._inv_round2(v_prev_vat),
      'prev_total_inc_vat', public._inv_round2(v_prev_inc),

      'delta_subtotal_ex_vat', v_delta_ex,
      'delta_vat_amount', v_delta_vat,
      'delta_total_inc_vat', v_delta_inc,

      'new_subtotal_ex_vat', public._inv_round2(v_new_ex),
      'new_vat_amount', public._inv_round2(v_new_vat),
      'new_total_inc_vat', public._inv_round2(v_new_inc),

      'invoice_pdf_force_regen_enqueued', v_pdf_jobs_enqueued,
      'run_at_utc', public._inv_iso_utc(v_now),
      'run_kind', 'REMOVE_NHSP_SHIFTS'
    ),
    null,
    p_actor_user_id
  );

  -- Generic “totals delta applied” audit (optional but recommended for unified reporting)
  if (coalesce(v_delta_ex,0) <> 0 or coalesce(v_delta_vat,0) <> 0 or coalesce(v_delta_inc,0) <> 0) then
    perform public._audit_insert(
      'invoice',
      p_invoice_id::text,
      'INVOICE_TOTALS_DELTA_APPLIED',
      jsonb_build_object(
        'invoice_no', v_invoice_no,
        'status', v_prev_status,
        'subtotal_ex_vat', public._inv_round2(v_prev_ex),
        'vat_amount', public._inv_round2(v_prev_vat),
        'total_inc_vat', public._inv_round2(v_prev_inc)
      ),
      jsonb_build_object(
        'invoice_id', p_invoice_id::text,
        'invoice_no', v_invoice_no,
        'run_at_utc', public._inv_iso_utc(v_now),
        'run_kind', 'REMOVE_NHSP_SHIFTS',

        'invoice_status_before', v_prev_status,
        'invoice_status_after', v_new_status,

        'delta_subtotal_ex_vat', v_delta_ex,
        'delta_vat_amount', v_delta_vat,
        'delta_total_inc_vat', v_delta_inc,

        'new_subtotal_ex_vat', public._inv_round2(v_new_ex),
        'new_vat_amount', public._inv_round2(v_new_vat),
        'new_total_inc_vat', public._inv_round2(v_new_inc),

        'timesheet_ids_this_run', to_jsonb(coalesce(v_removed_ts_ids, array[]::uuid[])),
        'source_keys_this_run', to_jsonb(coalesce(v_removed_source_keys, array[]::text[])),
        'line_count_this_run', coalesce(v_removed_line_count,0),

        'invoice_pdf_force_regen_enqueued', v_pdf_jobs_enqueued
      ),
      null,
      p_actor_user_id
    );
  end if;

  -- Return updated invoice totals
  select u.id, u.subtotal_ex_vat, u.vat_amount, u.total_inc_vat
  into invoice_id, subtotal_ex_vat, vat_amount, total_inc_vat
  from public.invoices u
  where u.id = p_invoice_id;

  return next;
end;
$$;
