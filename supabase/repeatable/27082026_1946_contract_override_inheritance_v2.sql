begin;

-- Contract override lifecycle:
--   * OFF means inherit from the Client. Nullable override columns are cleared.
--   * ON means a complete Contract snapshot. Any missing governed values are
--     seeded from the latest Client settings row.
--
-- Four historical Contract columns are NOT NULL (auto_invoice,
-- require_reference_to_pay, require_reference_to_invoice and self_bill). Their
-- stored values are ignored while overrideclientsettings is false, so the OFF
-- branch gives them harmless storage defaults instead of attempting to store
-- NULL. A later OFF -> ON transition is seeded afresh from Client settings.

create or replace function public.contracts_enforce_overrideclientsettings()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_no_timesheet_required boolean;
  v_daily_calc_of_invoices boolean;
  v_group_nightsat_sunbh boolean;
  v_is_nhsp boolean;
  v_autoprocess_hr boolean;
  v_requires_hr boolean;
  v_hr_attach_to_invoice boolean;
  v_ts_attach_to_invoice boolean;
  v_reference_number_required_to_issue_invoice boolean;
  v_send_manual_invoices_to_different_email boolean;
  v_manual_invoices_alt_email_address text;
  v_default_submission_mode public.submission_mode_enum;
  v_timesheet_break_entry_mode public.timesheet_break_entry_mode_enum;
  v_auto_invoice boolean;
  v_require_reference_to_pay boolean;
  v_require_reference_to_invoice boolean;
  v_self_bill boolean;
begin
  new.overrideclientsettings := coalesce(new.overrideclientsettings, false);

  if new.overrideclientsettings = false then
    new.no_timesheet_required := null;
    new.daily_calc_of_invoices := null;
    new.group_nightsat_sunbh := null;
    new.is_nhsp := null;
    new.autoprocess_hr := null;
    new.requires_hr := null;
    new.hr_attach_to_invoice := null;
    new.ts_attach_to_invoice := null;
    new.reference_number_required_to_issue_invoice := null;
    new.send_manual_invoices_to_different_email := null;
    new.manual_invoices_alt_email_address := null;
    new.default_submission_mode := null;
    new.timesheet_break_entry_mode := null;

    -- These legacy columns are NOT NULL. Their values have no authority while
    -- the override switch is off and are replaced from Client settings on the
    -- next saved OFF -> ON transition.
    new.auto_invoice := coalesce(new.auto_invoice, false);
    new.require_reference_to_pay := coalesce(new.require_reference_to_pay, false);
    new.require_reference_to_invoice := coalesce(new.require_reference_to_invoice, false);
    new.self_bill := coalesce(new.self_bill, false);
    return new;
  end if;

  select
    cs.no_timesheet_required,
    cs.daily_calc_of_invoices,
    cs.group_nightsat_sunbh,
    cs.is_nhsp,
    cs.autoprocess_hr,
    cs.requires_hr,
    cs.hr_attach_to_invoice,
    cs.ts_attach_to_invoice,
    cs.reference_number_required_to_issue_invoice,
    cs.send_manual_invoices_to_different_email,
    cs.manual_invoices_alt_email_address,
    cs.default_submission_mode,
    cs.timesheet_break_entry_mode,
    cs.auto_invoice_default,
    cs.pay_reference_required,
    cs.invoice_reference_required,
    cs.self_bill_no_invoices_sent
  into
    v_no_timesheet_required,
    v_daily_calc_of_invoices,
    v_group_nightsat_sunbh,
    v_is_nhsp,
    v_autoprocess_hr,
    v_requires_hr,
    v_hr_attach_to_invoice,
    v_ts_attach_to_invoice,
    v_reference_number_required_to_issue_invoice,
    v_send_manual_invoices_to_different_email,
    v_manual_invoices_alt_email_address,
    v_default_submission_mode,
    v_timesheet_break_entry_mode,
    v_auto_invoice,
    v_require_reference_to_pay,
    v_require_reference_to_invoice,
    v_self_bill
  from public.client_settings cs
  where cs.client_id = new.client_id
  order by cs.effective_from desc nulls last, cs.updated_at desc
  limit 1;

  new.no_timesheet_required := coalesce(new.no_timesheet_required, v_no_timesheet_required, false);
  new.daily_calc_of_invoices := coalesce(new.daily_calc_of_invoices, v_daily_calc_of_invoices, false);
  new.group_nightsat_sunbh := coalesce(new.group_nightsat_sunbh, v_group_nightsat_sunbh, false);
  new.is_nhsp := coalesce(new.is_nhsp, v_is_nhsp, false);
  new.autoprocess_hr := coalesce(new.autoprocess_hr, v_autoprocess_hr, false);
  new.requires_hr := coalesce(new.requires_hr, v_requires_hr, false);
  new.hr_attach_to_invoice := coalesce(new.hr_attach_to_invoice, v_hr_attach_to_invoice, true);
  new.ts_attach_to_invoice := coalesce(new.ts_attach_to_invoice, v_ts_attach_to_invoice, true);
  new.reference_number_required_to_issue_invoice := coalesce(
    new.reference_number_required_to_issue_invoice,
    v_reference_number_required_to_issue_invoice,
    false
  );
  new.send_manual_invoices_to_different_email := coalesce(
    new.send_manual_invoices_to_different_email,
    v_send_manual_invoices_to_different_email,
    false
  );
  new.default_submission_mode := coalesce(new.default_submission_mode, v_default_submission_mode, 'ELECTRONIC');
  new.timesheet_break_entry_mode := coalesce(
    new.timesheet_break_entry_mode,
    v_timesheet_break_entry_mode,
    'START_END_TIMES'
  );
  new.auto_invoice := coalesce(new.auto_invoice, v_auto_invoice, false);
  new.require_reference_to_pay := coalesce(new.require_reference_to_pay, v_require_reference_to_pay, false);
  new.require_reference_to_invoice := coalesce(
    new.require_reference_to_invoice,
    v_require_reference_to_invoice,
    false
  );
  new.self_bill := coalesce(new.self_bill, v_self_bill, false);

  if new.send_manual_invoices_to_different_email = true then
    if new.manual_invoices_alt_email_address is null or btrim(new.manual_invoices_alt_email_address) = '' then
      new.manual_invoices_alt_email_address := v_manual_invoices_alt_email_address;
    end if;
    if new.manual_invoices_alt_email_address is null or btrim(new.manual_invoices_alt_email_address) = '' then
      raise exception 'manual_invoices_alt_email_address is required when send_manual_invoices_to_different_email is true';
    end if;
    new.manual_invoices_alt_email_address := btrim(new.manual_invoices_alt_email_address);
  else
    new.manual_invoices_alt_email_address := null;
  end if;

  return new;
end;
$$;

commit;
