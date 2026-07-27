-- CloudTMS TEST Invoice Async V8/V2 statement-level candidate revision gates.
-- Install after the V8 trigger helper repeatables. Every UPDATE is projected to
-- the exact candidate-visible field list before either revision is advanced.

do $migration$
declare
  v_item record;
  v_argument_list text;
  v_trigger_base text;
begin
  if to_regprocedure('private._invoice_candidate_revision_trigger_v2()') is null then
    raise exception using
      errcode = '55000',
      message = 'INVOICE_ASYNC_V8_TRIGGER_HELPER_NOT_INSTALLED';
  end if;

  for v_item in
    select manifest.table_name, manifest.fields
    from jsonb_to_recordset($manifest$
    [
      {"table_name":"contracts","fields":["candidate_id","client_id","start_date","end_date","pay_method_snapshot","rates_json","std_hours_json","default_submission_mode","week_ending_weekday_snapshot","auto_invoice","require_reference_to_invoice","mileage_charge_rate","additional_rates_json","self_bill","weekly_timesheet_source","no_timesheet_required","daily_calc_of_invoices","group_nightsat_sunbh","is_nhsp","autoprocess_hr","requires_hr","hr_attach_to_invoice","ts_attach_to_invoice","overrideclientsettings","reference_number_required_to_issue_invoice","send_manual_invoices_to_different_email","manual_invoices_alt_email_address","is_ad_hoc","healthroster_import_auto_authorise_override","nhsp_import_auto_authorise_override"]},
      {"table_name":"contract_weeks","fields":["contract_id","week_ending_date","additional_seq","status","submission_mode_snapshot","timesheet_id","day_entries_json","totals_json","planned_schedule_json","is_adjustment","enforce_day_partition","allowed_days_mask","split_boundary_date","split_group_key"]},
      {"table_name":"client_settings","fields":["client_id","vat_rate_pct","effective_from","hr_validation_required","ts_reference_required","week_ending_weekday","autoprocess_hr","invoice_reference_required","default_submission_mode","is_nhsp","self_bill_no_invoices_sent","daily_calc_of_invoices","no_timesheet_required","group_nightsat_sunbh","requires_hr","hr_attach_to_invoice","ts_attach_to_invoice","auto_invoice_default","send_manual_invoices_to_different_email","manual_invoices_alt_email_address","invoice_consolidation_mode","reference_number_required_to_issue_invoice","reversal_complete_financials_date","reversal_replacement_financials_date"]},
      {"table_name":"clients","fields":["name","invoice_address","primary_invoice_email","vat_chargeable","payment_terms_days","ts_queries_email","client_address","contact_email"]},
      {"table_name":"settings_finance_windows","fields":["date_from","date_to","vat_rate_pct","mileage_charge_defaults"]},
      {"table_name":"nhsp_shifts","fields":["external_row_key","latest_import_id","candidate_id","client_id","contract_id","timesheet_id","work_date","ward","start_utc","end_utc","break_mins","pay_minutes","pay_amount_snapshot","charge_amount_snapshot","invoice_status","defer_until_run_after","invoice_id","source_system","hr_request_id","held_back_reason","assignment_code","ref_num","week_ending_date","cancelled_at_utc","cancelled_by_import_id","cancelled_reason"]},
      {"table_name":"invoices","fields":["client_id","invoice_no","type","original_invoice_id","subtotal_ex_vat","vat_amount","total_inc_vat","due_at_utc","notes","do_not_send","header_snapshot_json","status","issued_at_utc","paid_at_utc","on_hold_reason","document_revision","document_state","preview_document_version_id","issued_document_version_id","active_document_operation_id","issue_state","active_issue_operation_id","last_document_error_json"]},
      {"table_name":"invoice_lines","fields":["invoice_id","timesheet_id","booking_id","source_key","description","hours_day","hours_night","hours_sat","hours_sun","hours_bh","pay_day","pay_night","pay_sat","pay_sun","pay_bh","charge_day","charge_night","charge_sat","charge_sun","charge_bh","total_pay_ex_vat","total_charge_ex_vat","vat_rate_pct","vat_amount","total_inc_vat","margin_ex_vat","meta_json"]},
      {"table_name":"invoice_hr_source_rows","fields":["invoice_id","source_system","import_id","header_rows","header_columns","rows_json"]},
      {"table_name":"timesheets","fields":["booking_id","occupant_key_norm","hospital_norm","ward_norm","job_title_norm","shift_label_norm","scheduled_start_iso","scheduled_end_iso","worked_start_iso","worked_end_iso","break_start_iso","break_end_iso","break_minutes","worked_minutes","week_ending_date","auth_name","auth_job_title","authorised_at_server","r2_nurse_key","r2_auth_key","img_sha256_nurse","img_sha256_auth","reference_number","status","version","is_current","revoked_at","contract_id","submission_mode","line_type","sheet_scope","actual_schedule_json","additional_units_week","additional_units_per_day","day_references_json","qr_signed_hash","qr_signed_at_utc","qr_status","qr_r2_key","candidate_hint_text","band","is_adjustment","parent_timesheet_id","correction_id","correction_kind","adjustment_origin","archived_at_utc","archived_by_user_id","archived_reason_code","document_revision","document_state","current_document_version_id","active_document_operation_id","manual_document_asset_id","last_document_error_json"]},
      {"table_name":"timesheets_financials","fields":["timesheet_id","timesheet_version","basis","is_current","is_stale","worked_start_iso","worked_end_iso","break_start_iso","break_end_iso","break_minutes","candidate_id","client_id","role","band","policy_snapshot_json","rate_source_refs_json","hours_day","hours_night","hours_sat","hours_sun","hours_bh","pay_day","pay_night","pay_sat","pay_sun","pay_bh","charge_day","charge_night","charge_sat","charge_sun","charge_bh","total_hours","total_pay_ex_vat","total_charge_ex_vat","margin_ex_vat","processing_status","expenses_pay_ex_vat","expenses_charge_ex_vat","expenses_description","expenses_evidence_manifest","mileage_pay_ex_vat","mileage_charge_ex_vat","mileage_pay_rate","mileage_charge_rate","mileage_evidence_manifest","actual_schedule_json","actual_minutes_by_day_json","additional_units_json","additional_pay_ex_vat","additional_charge_ex_vat","additional_margin_ex_vat","invoice_breakdown_json","nhsp_import_id","has_rate_issue","hr_crosscheck_status","hr_crosscheck_issues","external_source_rows_json","mileage_units","travel_pay_ex_vat","travel_charge_ex_vat","accommodation_pay_ex_vat","accommodation_charge_ex_vat","other_pay_ex_vat","other_charge_ex_vat","stale_reason","pay_method","locked_by_invoice_id","unlocked_by_credit_note_id","po_number","pay_on_hold","pay_on_hold_reason","has_pay_channel_issue"]},
      {"table_name":"timesheet_evidence","fields":["timesheet_id","kind","storage_key","source_revision","display_name","document_asset_id","processing_state","processing_error_json"]},
      {"table_name":"invoice_document_versions","fields":["entity_type","entity_id","purpose","operation_id","source_revision","template_version","status","r2_key","sha256","size_bytes","page_count","ready_at_utc","verified_at_utc","superseded_at_utc","error_json"]},
      {"table_name":"invoice_document_assets","fields":["source_kind","source_id","source_revision","declared_media_type","detected_media_type","original_sha256","original_size_bytes","status","normalised_manifest_hash","normalised_r2_key","normalised_sha256","normalised_size_bytes","normalised_page_count","operation_id","error_json","ready_at_utc"]},
      {"table_name":"invoice_operations","fields":["parent_operation_id","operation_type","entity_type","entity_id","status","phase","source_revision","template_version","control_version","result_json","error_json"]},
      {"table_name":"invoice_operation_chunks","fields":["operation_id","chunk_type","entity_type","entity_id","document_version_id","document_asset_id","input_document_version_id","status","phase","replaced_by_chunk_id","payload_json","result_json","error_json"]}
    ]
    $manifest$::jsonb) as manifest(table_name text, fields jsonb)
  loop
    if to_regclass(format('public.%I', v_item.table_name)) is null then
      raise exception using
        errcode = '42P01',
        message = 'INVOICE_ASYNC_V8_TRIGGER_TABLE_MISSING:' || v_item.table_name;
    end if;

    select string_agg(quote_literal(field_name), ',' order by ordinal)
    into v_argument_list
    from jsonb_array_elements_text(v_item.fields)
      with ordinality field(field_name, ordinal);

    v_trigger_base := left(
      'trg_invoice_candidate_revision_v2_' || v_item.table_name,
      58
    );

    execute format(
      'drop trigger if exists %I on public.%I',
      v_trigger_base || '_i',
      v_item.table_name
    );
    execute format(
      'create trigger %I after insert on public.%I referencing new table as new_rows for each statement execute function private._invoice_candidate_revision_trigger_v2(%L,%L,%s)',
      v_trigger_base || '_i',
      v_item.table_name,
      'true',
      'true',
      v_argument_list
    );

    execute format(
      'drop trigger if exists %I on public.%I',
      v_trigger_base || '_u',
      v_item.table_name
    );
    execute format(
      'create trigger %I after update on public.%I referencing old table as old_rows new table as new_rows for each statement execute function private._invoice_candidate_revision_trigger_v2(%L,%L,%s)',
      v_trigger_base || '_u',
      v_item.table_name,
      'true',
      'true',
      v_argument_list
    );

    execute format(
      'drop trigger if exists %I on public.%I',
      v_trigger_base || '_d',
      v_item.table_name
    );
    execute format(
      'create trigger %I after delete on public.%I referencing old table as old_rows for each statement execute function private._invoice_candidate_revision_trigger_v2(%L,%L,%s)',
      v_trigger_base || '_d',
      v_item.table_name,
      'true',
      'true',
      v_argument_list
    );
  end loop;

  if to_regprocedure('private._invoice_result_page_revision_trigger_v2()') is null then
    raise exception using
      errcode = '55000',
      message = 'INVOICE_ASYNC_V8_RESULT_REVISION_HELPER_NOT_INSTALLED';
  end if;

  drop trigger if exists trg_invoice_result_page_revision_v2_i
    on public.invoice_operation_chunks;
  create trigger trg_invoice_result_page_revision_v2_i
  after insert on public.invoice_operation_chunks
  referencing new table as new_rows
  for each statement
  execute function private._invoice_result_page_revision_trigger_v2();

  drop trigger if exists trg_invoice_result_page_revision_v2_u
    on public.invoice_operation_chunks;
  create trigger trg_invoice_result_page_revision_v2_u
  after update on public.invoice_operation_chunks
  referencing old table as old_rows new table as new_rows
  for each statement
  execute function private._invoice_result_page_revision_trigger_v2();

  drop trigger if exists trg_invoice_result_page_revision_v2_d
    on public.invoice_operation_chunks;
  create trigger trg_invoice_result_page_revision_v2_d
  after delete on public.invoice_operation_chunks
  referencing old table as old_rows
  for each statement
  execute function private._invoice_result_page_revision_trigger_v2();
end;
$migration$;
