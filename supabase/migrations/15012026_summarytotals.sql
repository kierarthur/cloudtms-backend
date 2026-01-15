-- ============================================================
-- CloudTMS: Summary Totals RPCs (Invoices / Timesheets / Contracts)
-- SAFE TO RE-RUN: CREATE OR REPLACE FUNCTION (idempotent)
--
-- These RPCs are designed to return totals for ALL rows matching the
-- same filter semantics used by the list endpoints / summary grids,
-- independent of paging.
--
-- Functions:
--   1) public.invoice_list_totals(p_filters jsonb)
--   2) public.timesheet_list_totals(p_filters jsonb)
--   3) public.contract_list_count(p_filters jsonb)   (optional)
--
-- Notes:
-- - Filters are passed as JSONB (same keys as your UI/backend).
-- - We deliberately do NOT apply paging (limit/offset) here.
-- ============================================================


-- ============================================================
-- 1) invoice_list_totals(filters jsonb)
-- Mirrors handleListInvoices() filter semantics:
--   client_id, status (enum list or legacy paid/unpaid), q (invoice_no ilike),
--   issued_from/to, due_from/to, created_from/to
-- Margin is computed as SUM(invoice_lines.margin_ex_vat) for matching invoices.
-- ============================================================
create or replace function public.invoice_list_totals(p_filters jsonb)
returns table (
  count_all bigint,
  subtotal_ex_vat_sum numeric,
  total_inc_vat_sum numeric,
  margin_ex_vat_sum numeric
)
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_client_id uuid := null;
  v_q text := null;

  v_issued_from timestamptz := null;
  v_issued_to   timestamptz := null;
  v_due_from    timestamptz := null;
  v_due_to      timestamptz := null;
  v_created_from timestamptz := null;
  v_created_to   timestamptz := null;

  v_status_raw jsonb := null;
  v_status_list text[] := null;
  v_legacy_paid_filter text := null; -- 'paid' | 'unpaid' | null
begin
  if p_filters is null then p_filters := '{}'::jsonb; end if;

  -- client_id
  begin
    if (p_filters ? 'client_id') and nullif(btrim(coalesce(p_filters->>'client_id','')), '') is not null then
      v_client_id := (p_filters->>'client_id')::uuid;
    end if;
  exception when others then
    v_client_id := null;
  end;

  -- q (partial invoice_no)
  v_q := nullif(btrim(coalesce(p_filters->>'q','')), '');

  -- dates (accept ISO date or timestamptz strings)
  begin if nullif(btrim(coalesce(p_filters->>'issued_from','')), '') is not null then v_issued_from := (p_filters->>'issued_from')::timestamptz; end if; exception when others then v_issued_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'issued_to','')), '') is not null then v_issued_to := (p_filters->>'issued_to')::timestamptz; end if; exception when others then v_issued_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'due_from','')), '') is not null then v_due_from := (p_filters->>'due_from')::timestamptz; end if; exception when others then v_due_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'due_to','')), '') is not null then v_due_to := (p_filters->>'due_to')::timestamptz; end if; exception when others then v_due_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'created_from','')), '') is not null then v_created_from := (p_filters->>'created_from')::timestamptz; end if; exception when others then v_created_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'created_to','')), '') is not null then v_created_to := (p_filters->>'created_to')::timestamptz; end if; exception when others then v_created_to := null; end;

  -- status logic:
  -- - p_filters.status may be array OR comma string OR single enum string OR 'paid'/'unpaid'
  if p_filters ? 'status' then
    v_status_raw := p_filters->'status';
  end if;

  if v_status_raw is not null then
    if jsonb_typeof(v_status_raw) = 'array' then
      select array_agg(upper(btrim(x))) into v_status_list
      from (
        select jsonb_array_elements_text(v_status_raw) as x
      ) s
      where nullif(btrim(coalesce(x,'')), '') is not null;
    else
      -- string / scalar
      v_status_list := array_remove(string_to_array(upper(btrim(coalesce(v_status_raw::text,''))), ','), '');
    end if;
  end if;

  -- legacy paid/unpaid filter support
  if v_status_list is not null and array_length(v_status_list,1) = 1 then
    if lower(v_status_list[1]) = 'paid' or v_status_list[1] = 'PAID' then
      v_legacy_paid_filter := 'paid';
      v_status_list := null;
    elsif lower(v_status_list[1]) = 'unpaid' or v_status_list[1] = 'UNPAID' then
      v_legacy_paid_filter := 'unpaid';
      v_status_list := null;
    end if;
  end if;

  -- Sanitize enum status list (DRAFT/ISSUED/ON_HOLD/PAID)
  if v_status_list is not null then
    select array_agg(s)
    into v_status_list
    from (
      select distinct replace(replace(replace(upper(btrim(x)),'(',''),')',''),',','') as s
      from unnest(v_status_list) x
    ) t
    where s in ('DRAFT','ISSUED','ON_HOLD','PAID');
    if v_status_list is null or coalesce(array_length(v_status_list,1),0) = 0 then
      v_status_list := null;
    end if;
  end if;

  return query
  with inv as (
    select
      i.id,
      i.subtotal_ex_vat,
      i.total_inc_vat
    from public.invoices i
    where (v_client_id is null or i.client_id = v_client_id)
      and (v_q is null or i.invoice_no ilike ('%'||v_q||'%'))
      and (v_issued_from is null or i.issued_at_utc >= v_issued_from)
      and (v_issued_to   is null or i.issued_at_utc <= v_issued_to)
      and (v_due_from    is null or i.due_at_utc >= v_due_from)
      and (v_due_to      is null or i.due_at_utc <= v_due_to)
      and (v_created_from is null or i.created_at >= v_created_from)
      and (v_created_to   is null or i.created_at <= v_created_to)
      and (
        v_legacy_paid_filter is null
        or (v_legacy_paid_filter = 'paid' and i.paid_at_utc is not null)
        or (v_legacy_paid_filter = 'unpaid' and i.paid_at_utc is null)
      )
      and (
        v_status_list is null
        or i.status::text = any(v_status_list)
      )
  ),
  m as (
    select
      l.invoice_id,
      sum(coalesce(l.margin_ex_vat,0))::numeric as margin_sum
    from public.invoice_lines l
    join inv on inv.id = l.invoice_id
    group by l.invoice_id
  )
  select
    count(*)::bigint as count_all,
    coalesce(sum(inv.subtotal_ex_vat),0)::numeric as subtotal_ex_vat_sum,
    coalesce(sum(inv.total_inc_vat),0)::numeric as total_inc_vat_sum,
    coalesce(sum(coalesce(m.margin_sum,0)),0)::numeric as margin_ex_vat_sum
  from inv
  left join m on m.invoice_id = inv.id;
end;
$$;


-- ============================================================
-- 2) timesheet_list_totals(filters jsonb)
-- Mirrors handleTimesheetsSummary() filter semantics against v_timesheets_summary.
-- Supported filter keys (subset):
--   client_id, candidate_id, summary_stage,
--   route_type (ALL/ELECTRONIC/MANUAL/NHSP/HEALTHROSTER/QR),
--   sheet_scope (ALL/WEEKLY/DAILY),
--   qr_status,
--   week_ending_from/to,
--   is_adjusted, is_qr, needs_attention,
--   candidate_paid (true), client_invoiced (true),
--   hr_issue (maps to hr_crosscheck_issues contains),
--   processing_status (comma list; ALL means no filter)
--   status_code (UI-only helper: NO_MATCH_ID/RATE_MISSING/PAY_CHAN_MISS/READY_FOR_HR/READY_FOR_INV)
-- Totals computed from v_timesheets_summary fields: total_pay_ex_vat, margin_ex_vat.
-- ============================================================
create or replace function public.timesheet_list_totals(p_filters jsonb)
returns table (
  count_all bigint,
  total_pay_ex_vat_sum numeric,
  margin_ex_vat_sum numeric
)
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_client_id uuid := null;
  v_candidate_id uuid := null;

  v_summary_stage text := null;

  v_route_type text := null;
  v_sheet_scope text := null;
  v_qr_status text := null;

  v_we_from date := null;
  v_we_to   date := null;

  v_is_adjusted text := null;
  v_is_qr text := null;
  v_needs_attention text := null;

  v_candidate_paid text := null;
  v_client_invoiced text := null;

  v_hr_issue text := null;

  v_proc_status_raw text := null;
  v_proc_list text[] := null;

  v_status_code text := null;
begin
  if p_filters is null then p_filters := '{}'::jsonb; end if;

  -- ids
  begin if nullif(btrim(coalesce(p_filters->>'client_id','')), '') is not null then v_client_id := (p_filters->>'client_id')::uuid; end if; exception when others then v_client_id := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'candidate_id','')), '') is not null then v_candidate_id := (p_filters->>'candidate_id')::uuid; end if; exception when others then v_candidate_id := null; end;

  v_summary_stage := nullif(btrim(coalesce(p_filters->>'summary_stage','')), '');
  if v_summary_stage is not null and upper(v_summary_stage) = 'ALL' then v_summary_stage := null; end if;

  v_route_type := nullif(btrim(coalesce(p_filters->>'route_type','')), '');
  if v_route_type is not null then v_route_type := upper(v_route_type); end if;
  if v_route_type = 'ALL' then v_route_type := null; end if;

  v_sheet_scope := nullif(btrim(coalesce(p_filters->>'sheet_scope','')), '');
  if v_sheet_scope is not null then v_sheet_scope := upper(v_sheet_scope); end if;
  if v_sheet_scope = 'ALL' then v_sheet_scope := null; end if;

  v_qr_status := nullif(btrim(coalesce(p_filters->>'qr_status','')), '');
  if v_qr_status is not null then v_qr_status := upper(v_qr_status); end if;

  begin if nullif(btrim(coalesce(p_filters->>'week_ending_from','')), '') is not null then v_we_from := (p_filters->>'week_ending_from')::date; end if; exception when others then v_we_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'week_ending_to','')), '') is not null then v_we_to := (p_filters->>'week_ending_to')::date; end if; exception when others then v_we_to := null; end;

  v_is_adjusted := nullif(btrim(coalesce(p_filters->>'is_adjusted','')), '');
  v_is_qr := nullif(btrim(coalesce(p_filters->>'is_qr','')), '');
  v_needs_attention := nullif(btrim(coalesce(p_filters->>'needs_attention','')), '');

  v_candidate_paid := nullif(btrim(coalesce(p_filters->>'candidate_paid','')), '');
  v_client_invoiced := nullif(btrim(coalesce(p_filters->>'client_invoiced','')), '');

  v_hr_issue := nullif(btrim(coalesce(p_filters->>'hr_issue','')), '');
  if v_hr_issue is not null then v_hr_issue := upper(v_hr_issue); end if;

  v_proc_status_raw := nullif(btrim(coalesce(p_filters->>'processing_status','')), '');
  if v_proc_status_raw is not null and upper(v_proc_status_raw) <> 'ALL' then
    v_proc_list := array_remove(string_to_array(upper(v_proc_status_raw), ','), '');
  else
    v_proc_list := null;
  end if;

  v_status_code := nullif(btrim(coalesce(p_filters->>'status_code','')), '');
  if v_status_code is not null then v_status_code := upper(v_status_code); end if;

  return query
  with base as (
    select *
    from public.v_timesheets_summary v
    where (v_client_id is null or v.client_id = v_client_id)
      and (v_candidate_id is null or v.candidate_id = v_candidate_id)
      and (v_summary_stage is null or upper(coalesce(v.summary_stage::text,'')) = upper(v_summary_stage))
      and (v_we_from is null or v.week_ending_date >= v_we_from)
      and (v_we_to   is null or v.week_ending_date <= v_we_to)

      -- route_type aggregation (matches backend mapping)
      and (
        v_route_type is null
        or (v_route_type = 'ELECTRONIC' and v.route_type in ('DAILY_ELECTRONIC','WEEKLY_ELECTRONIC'))
        or (v_route_type = 'MANUAL' and v.route_type in ('DAILY_MANUAL','WEEKLY_MANUAL'))
        or (v_route_type = 'NHSP' and v.route_type in ('WEEKLY_NHSP','WEEKLY_NHSP_ADJUSTMENT'))
        or (v_route_type = 'HEALTHROSTER' and v.route_type = 'WEEKLY_HEALTHROSTER')
        or (v_route_type = 'QR' and coalesce(v.is_qr,false) = true)
        or (v_route_type not in ('ELECTRONIC','MANUAL','NHSP','HEALTHROSTER','QR') and v.route_type = v_route_type)
      )

      and (v_sheet_scope is null or upper(coalesce(v.sheet_scope::text,'')) = v_sheet_scope)
      and (v_qr_status is null or upper(coalesce(v.qr_status::text,'')) = v_qr_status)

      and (
        v_is_adjusted is null
        or (lower(v_is_adjusted) = 'true' and coalesce(v.is_adjusted,false) = true)
        or (lower(v_is_adjusted) = 'false' and coalesce(v.is_adjusted,false) = false)
      )
      and (
        v_is_qr is null
        or (lower(v_is_qr) = 'true' and coalesce(v.is_qr,false) = true)
        or (lower(v_is_qr) = 'false' and coalesce(v.is_qr,false) = false)
      )
      and (
        v_needs_attention is null
        or (lower(v_needs_attention) = 'true' and coalesce(v.needs_attention,false) = true)
        or (lower(v_needs_attention) = 'false' and coalesce(v.needs_attention,false) = false)
      )

      and (
        v_candidate_paid is null
        or (lower(v_candidate_paid) = 'true' and v.paid_at_utc is not null)
      )
      and (
        v_client_invoiced is null
        or (lower(v_client_invoiced) = 'true' and v.locked_by_invoice_id is not null)
      )

      and (
        v_hr_issue is null
        or (v.hr_crosscheck_issues is not null and v_hr_issue = any(v.hr_crosscheck_issues))
      )

      and (
        v_proc_list is null
        or upper(coalesce(v.processing_status::text,'')) = any(v_proc_list)
      )
  ),
  effective as (
    select *
    from base b
    where
      -- Mirror FE "status_code" client-side filtering so totals match what user sees
      (
        v_status_code is null
        or v_status_code = 'ALL'
        or (v_status_code = 'NO_MATCH_ID' and (b.candidate_id is null or b.client_id is null))
        or (v_status_code = 'RATE_MISSING' and b.has_rate_issue = true)
        or (v_status_code = 'PAY_CHAN_MISS' and b.has_pay_channel_issue = true)
        or (v_status_code = 'READY_FOR_HR' and upper(coalesce(b.processing_status::text,'')) = 'READY_FOR_HR')
        or (v_status_code = 'READY_FOR_INV' and upper(coalesce(b.processing_status::text,'')) = 'READY_FOR_INVOICE')
        or (v_status_code not in ('NO_MATCH_ID','RATE_MISSING','PAY_CHAN_MISS','READY_FOR_HR','READY_FOR_INV'))
      )
  )
  select
    count(*)::bigint as count_all,
    coalesce(sum(coalesce(e.total_pay_ex_vat,0)),0)::numeric as total_pay_ex_vat_sum,
    coalesce(sum(coalesce(e.margin_ex_vat,0)),0)::numeric as margin_ex_vat_sum
  from effective e;
end;
$$;


-- ============================================================
-- 3) contract_list_count(filters jsonb)  (optional)
-- Mirrors handleContractsList() core filtering and status=active/completed/unassigned.
-- This RPC returns ONLY count for all matching contracts (no paging).
-- ============================================================
create or replace function public.contract_list_count(p_filters jsonb)
returns table (
  count_all bigint
)
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_candidate_id uuid := null;
  v_client_id uuid := null;

  v_pay_method text := null;
  v_role_like text := null;
  v_band text := null;

  v_candidate_name_like text := null;
  v_client_name_like text := null;

  v_active_on date := null;

  v_auto_invoice text := null;
  v_q text := null;

  v_default_submission_mode text := null;
  v_week_ending_weekday_snapshot int := null;

  v_require_ref_pay text := null;
  v_require_ref_inv text := null;

  v_has_custom_labels text := null;

  v_created_from timestamptz := null;
  v_created_to timestamptz := null;
  v_updated_from timestamptz := null;
  v_updated_to timestamptz := null;

  v_start_from date := null;
  v_start_to date := null;
  v_end_from date := null;
  v_end_to date := null;

  v_mileage_pay_rate numeric := null;
  v_mileage_charge_rate numeric := null;

  v_status text := null; -- all|active|completed|unassigned

  v_ids jsonb := null;
  v_ids_arr uuid[] := null;

  INCOMPLETE_STATUSES text[] := array['OPEN','PLANNED','SUBMITTED','AUTHORISED'];
begin
  if p_filters is null then p_filters := '{}'::jsonb; end if;

  begin if nullif(btrim(coalesce(p_filters->>'candidate_id','')), '') is not null then v_candidate_id := (p_filters->>'candidate_id')::uuid; end if; exception when others then v_candidate_id := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'client_id','')), '') is not null then v_client_id := (p_filters->>'client_id')::uuid; end if; exception when others then v_client_id := null; end;

  v_pay_method := nullif(btrim(coalesce(p_filters->>'pay_method_snapshot','')), '');
  if v_pay_method is not null then v_pay_method := upper(v_pay_method); end if;

  if nullif(btrim(coalesce(p_filters->>'role','')), '') is not null then
    v_role_like := '%' || (p_filters->>'role') || '%';
  end if;

  v_band := nullif(btrim(coalesce(p_filters->>'band','')), '');

  if nullif(btrim(coalesce(p_filters->>'candidate_name','')), '') is not null then
    v_candidate_name_like := '%' || (p_filters->>'candidate_name') || '%';
  end if;
  if nullif(btrim(coalesce(p_filters->>'client_name','')), '') is not null then
    v_client_name_like := '%' || (p_filters->>'client_name') || '%';
  end if;

  begin if nullif(btrim(coalesce(p_filters->>'active_on','')), '') is not null then v_active_on := (p_filters->>'active_on')::date; end if; exception when others then v_active_on := null; end;

  v_auto_invoice := nullif(btrim(coalesce(p_filters->>'auto_invoice','')), '');

  v_q := nullif(btrim(coalesce(p_filters->>'q','')), '');

  v_default_submission_mode := nullif(btrim(coalesce(p_filters->>'default_submission_mode','')), '');
  if v_default_submission_mode is null then
    v_default_submission_mode := nullif(btrim(coalesce(p_filters->>'submission_mode','')), '');
  end if;
  if v_default_submission_mode is not null then v_default_submission_mode := upper(v_default_submission_mode); end if;

  begin if nullif(btrim(coalesce(p_filters->>'week_ending_weekday_snapshot','')), '') is not null then v_week_ending_weekday_snapshot := (p_filters->>'week_ending_weekday_snapshot')::int; end if; exception when others then v_week_ending_weekday_snapshot := null; end;

  v_require_ref_pay := nullif(btrim(coalesce(p_filters->>'require_reference_to_pay','')), '');
  v_require_ref_inv := nullif(btrim(coalesce(p_filters->>'require_reference_to_invoice','')), '');

  v_has_custom_labels := nullif(btrim(coalesce(p_filters->>'has_custom_labels','')), '');

  begin if nullif(btrim(coalesce(p_filters->>'created_from','')), '') is not null then v_created_from := (p_filters->>'created_from')::timestamptz; end if; exception when others then v_created_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'created_to','')), '') is not null then v_created_to := (p_filters->>'created_to')::timestamptz; end if; exception when others then v_created_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'updated_from','')), '') is not null then v_updated_from := (p_filters->>'updated_from')::timestamptz; end if; exception when others then v_updated_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'updated_to','')), '') is not null then v_updated_to := (p_filters->>'updated_to')::timestamptz; end if; exception when others then v_updated_to := null; end;

  begin if nullif(btrim(coalesce(p_filters->>'start_date_from','')), '') is not null then v_start_from := (p_filters->>'start_date_from')::date; end if; exception when others then v_start_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'start_date_to','')), '') is not null then v_start_to := (p_filters->>'start_date_to')::date; end if; exception when others then v_start_to := null; end;

  begin if nullif(btrim(coalesce(p_filters->>'end_date_from','')), '') is not null then v_end_from := (p_filters->>'end_date_from')::date; end if; exception when others then v_end_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'end_date_to','')), '') is not null then v_end_to := (p_filters->>'end_date_to')::date; end if; exception when others then v_end_to := null; end;

  begin if nullif(btrim(coalesce(p_filters->>'mileage_pay_rate','')), '') is not null then v_mileage_pay_rate := (p_filters->>'mileage_pay_rate')::numeric; end if; exception when others then v_mileage_pay_rate := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'mileage_charge_rate','')), '') is not null then v_mileage_charge_rate := (p_filters->>'mileage_charge_rate')::numeric; end if; exception when others then v_mileage_charge_rate := null; end;

  v_status := lower(nullif(btrim(coalesce(p_filters->>'status','')), ''));
  if v_status is null then v_status := 'all'; end if;

  -- ids filter (focus selections): p_filters.ids can be array of uuids
  if p_filters ? 'ids' then
    v_ids := p_filters->'ids';
    if v_ids is not null and jsonb_typeof(v_ids)='array' then
      select array_agg((x)::uuid) into v_ids_arr
      from jsonb_array_elements_text(v_ids) x
      where nullif(btrim(coalesce(x,'')), '') is not null;
    end if;
  end if;

  return query
  with base as (
    select
      c.*,
      cand.display_name as cand_display,
      cand.first_name as cand_first,
      cand.last_name as cand_last,
      cli.name as cli_name
    from public.contracts c
    left join public.candidates cand on cand.id = c.candidate_id
    left join public.clients cli on cli.id = c.client_id
    where (v_ids_arr is null or c.id = any(v_ids_arr))
      and (v_candidate_id is null or c.candidate_id = v_candidate_id)
      and (v_client_id is null or c.client_id = v_client_id)
      and (v_pay_method is null or upper(coalesce(c.pay_method_snapshot::text,'')) = v_pay_method)
      and (v_band is null or c.band = v_band)
      and (v_role_like is null or c.role ilike v_role_like)
      and (v_active_on is null or (c.start_date <= v_active_on and c.end_date >= v_active_on))
      and (
        v_auto_invoice is null
        or (lower(v_auto_invoice) = 'true' and c.auto_invoice = true)
        or (lower(v_auto_invoice) = 'false' and c.auto_invoice = false)
      )
      and (v_default_submission_mode is null or upper(coalesce(c.default_submission_mode::text,'')) = v_default_submission_mode)
      and (v_week_ending_weekday_snapshot is null or c.week_ending_weekday_snapshot = v_week_ending_weekday_snapshot)
      and (
        v_require_ref_pay is null
        or (lower(v_require_ref_pay) = 'true' and c.require_reference_to_pay = true)
        or (lower(v_require_ref_pay) = 'false' and c.require_reference_to_pay = false)
      )
      and (
        v_require_ref_inv is null
        or (lower(v_require_ref_inv) = 'true' and c.require_reference_to_invoice = true)
        or (lower(v_require_ref_inv) = 'false' and c.require_reference_to_invoice = false)
      )
      and (
        v_has_custom_labels is null
        or (lower(v_has_custom_labels) = 'true' and c.bucket_labels_json is not null)
        or (lower(v_has_custom_labels) = 'false' and c.bucket_labels_json is null)
      )
      and (v_created_from is null or c.created_at >= v_created_from)
      and (v_created_to is null or c.created_at <= v_created_to)
      and (v_updated_from is null or c.updated_at >= v_updated_from)
      and (v_updated_to is null or c.updated_at <= v_updated_to)
      and (v_start_from is null or c.start_date >= v_start_from)
      and (v_start_to is null or c.start_date <= v_start_to)
      and (v_end_from is null or c.end_date >= v_end_from)
      and (v_end_to is null or c.end_date <= v_end_to)
      and (v_mileage_pay_rate is null or c.mileage_pay_rate = v_mileage_pay_rate)
      and (v_mileage_charge_rate is null or c.mileage_charge_rate = v_mileage_charge_rate)

      and (
        v_candidate_name_like is null
        or (coalesce(cand.display_name,'') ilike v_candidate_name_like)
        or (coalesce(cand.first_name,'') ilike v_candidate_name_like)
        or (coalesce(cand.last_name,'') ilike v_candidate_name_like)
      )
      and (
        v_client_name_like is null
        or (coalesce(cli.name,'') ilike v_client_name_like)
      )
      and (
        v_q is null
        or (
          coalesce(cli.name,'') ilike ('%'||v_q||'%')
          or coalesce(cand.display_name,'') ilike ('%'||v_q||'%')
          or coalesce(cand.first_name,'') ilike ('%'||v_q||'%')
          or coalesce(cand.last_name,'') ilike ('%'||v_q||'%')
          or coalesce(c.role,'') ilike ('%'||v_q||'%')
        )
      )
  ),
  filtered as (
    select b.*
    from base b
    where
      -- status semantics:
      (v_status = 'all')
      or (v_status = 'unassigned' and b.candidate_id is null)
      or (
        v_status = 'active'
        and exists (
          select 1
          from public.contract_weeks cw
          where cw.contract_id = b.id
            and upper(cw.status::text) = any(INCOMPLETE_STATUSES)
        )
      )
      or (
        v_status = 'completed'
        and b.candidate_id is not null
        and not exists (
          select 1
          from public.contract_weeks cw
          where cw.contract_id = b.id
            and upper(cw.status::text) = any(INCOMPLETE_STATUSES)
        )
      )
  )
  select count(*)::bigint as count_all
  from filtered;
end;
$$;
