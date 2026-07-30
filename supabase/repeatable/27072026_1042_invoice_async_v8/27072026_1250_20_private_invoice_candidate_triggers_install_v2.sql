create or replace function private._invoice_candidate_triggers_install_v2()
returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_item record;
  v_argument_list text;
  v_trigger_base text;
  v_installed integer := 0;
  v_manifest_rows jsonb := '[]'::jsonb;
begin
  if to_regprocedure('private._invoice_candidate_revision_trigger_v2()') is null
     or to_regprocedure('private._invoice_result_page_revision_trigger_v2()') is null then
    raise exception using
      errcode = '55000',
      message = 'INVOICE_ASYNC_V8_TRIGGER_HELPER_NOT_INSTALLED';
  end if;

  for v_item in
    select manifest.table_name, manifest.bump_generate,
      manifest.bump_issue, manifest.fields,
      coalesce(manifest.json_paths,'[]'::jsonb) json_paths
    from jsonb_to_recordset($manifest$
    [
      {"table_name":"contracts","bump_generate":true,"bump_issue":true,"fields":["candidate_id","client_id","start_date","end_date","pay_method_snapshot","rates_json","std_hours_json","default_submission_mode","week_ending_weekday_snapshot","auto_invoice","require_reference_to_invoice","mileage_charge_rate","additional_rates_json","self_bill","weekly_timesheet_source","no_timesheet_required","daily_calc_of_invoices","group_nightsat_sunbh","is_nhsp","autoprocess_hr","requires_hr","hr_attach_to_invoice","ts_attach_to_invoice","overrideclientsettings","reference_number_required_to_issue_invoice","send_manual_invoices_to_different_email","manual_invoices_alt_email_address","is_ad_hoc","healthroster_import_auto_authorise_override","nhsp_import_auto_authorise_override"]},
      {"table_name":"contract_weeks","bump_generate":true,"bump_issue":true,"fields":["contract_id","week_ending_date","additional_seq","status","submission_mode_snapshot","timesheet_id","day_entries_json","totals_json","planned_schedule_json","is_adjustment","enforce_day_partition","allowed_days_mask","split_boundary_date","split_group_key"]},
      {"table_name":"client_settings","bump_generate":true,"bump_issue":true,"fields":["client_id","vat_rate_pct","effective_from","hr_validation_required","ts_reference_required","week_ending_weekday","autoprocess_hr","invoice_reference_required","default_submission_mode","is_nhsp","self_bill_no_invoices_sent","daily_calc_of_invoices","no_timesheet_required","group_nightsat_sunbh","requires_hr","hr_attach_to_invoice","ts_attach_to_invoice","auto_invoice_default","send_manual_invoices_to_different_email","manual_invoices_alt_email_address","invoice_consolidation_mode","reference_number_required_to_issue_invoice","reversal_complete_financials_date","reversal_replacement_financials_date"]},
      {"table_name":"clients","bump_generate":true,"bump_issue":true,"fields":["name","invoice_address","primary_invoice_email","vat_chargeable","payment_terms_days","ts_queries_email","client_address","contact_email"]},
      {"table_name":"settings_defaults","bump_generate":true,"bump_issue":true,"fields":["vat_registration_number","agency_name","agency_logo","registered_address","company_reg_number","bank_name","bank_sort_code","bank_account_number","finance_email","hr_attach_to_invoice","ts_attach_to_invoice","invoice_document_presentation_json","timesheet_header_json","timesheet_footer_json","temporary_worker_declaration_json","client_declaration_json"]},
      {"table_name":"settings_finance_windows","bump_generate":true,"bump_issue":false,"fields":["date_from","date_to","vat_rate_pct","mileage_charge_defaults"]},
      {"table_name":"candidates","bump_generate":true,"bump_issue":true,"fields":["first_name","last_name","display_name","active","key_norm"]},
      {"table_name":"nhsp_shifts","bump_generate":true,"bump_issue":false,"fields":["external_row_key","latest_import_id","candidate_id","client_id","contract_id","timesheet_id","work_date","ward","start_utc","end_utc","break_mins","pay_minutes","pay_amount_snapshot","charge_amount_snapshot","invoice_status","defer_until_run_after","invoice_id","source_system","hr_request_id","held_back_reason","assignment_code","ref_num","week_ending_date","cancelled_at_utc","cancelled_by_import_id","cancelled_reason"]},
      {"table_name":"invoices","bump_generate":true,"bump_issue":true,"fields":["client_id","invoice_no","type","original_invoice_id","subtotal_ex_vat","vat_amount","total_inc_vat","due_at_utc","notes","do_not_send","header_snapshot_json","status","issued_at_utc","paid_at_utc","on_hold_reason","document_revision","document_state","preview_document_version_id","issued_document_version_id","active_document_operation_id","issue_state","active_issue_operation_id","last_document_error_json"]},
      {"table_name":"invoice_lines","bump_generate":true,"bump_issue":true,"fields":["invoice_id","timesheet_id","booking_id","source_key","description","hours_day","hours_night","hours_sat","hours_sun","hours_bh","pay_day","pay_night","pay_sat","pay_sun","pay_bh","charge_day","charge_night","charge_sat","charge_sun","charge_bh","total_pay_ex_vat","total_charge_ex_vat","vat_rate_pct","vat_amount","total_inc_vat","margin_ex_vat","meta_json"]},
      {"table_name":"invoice_hr_source_rows","bump_generate":false,"bump_issue":true,"fields":["invoice_id","source_system","import_id","header_rows","header_columns","rows_json"]},
      {"table_name":"timesheets","bump_generate":true,"bump_issue":true,"fields":["booking_id","occupant_key_norm","hospital_norm","ward_norm","job_title_norm","shift_label_norm","scheduled_start_iso","scheduled_end_iso","worked_start_iso","worked_end_iso","break_start_iso","break_end_iso","break_minutes","worked_minutes","week_ending_date","auth_name","auth_job_title","authorised_at_server","r2_nurse_key","r2_auth_key","img_sha256_nurse","img_sha256_auth","reference_number","status","version","is_current","revoked_at","contract_id","submission_mode","line_type","sheet_scope","actual_schedule_json","additional_units_week","additional_units_per_day","day_references_json","qr_signed_hash","qr_signed_at_utc","qr_status","qr_r2_key","candidate_hint_text","band","is_adjustment","parent_timesheet_id","correction_id","correction_kind","adjustment_origin","archived_at_utc","archived_by_user_id","archived_reason_code","document_revision","document_state","current_document_version_id","active_document_operation_id","manual_document_asset_id","last_document_error_json"]},
      {"table_name":"timesheets_financials","bump_generate":true,"bump_issue":true,"fields":["timesheet_id","timesheet_version","basis","is_current","is_stale","worked_start_iso","worked_end_iso","break_start_iso","break_end_iso","break_minutes","candidate_id","client_id","role","band","policy_snapshot_json","rate_source_refs_json","hours_day","hours_night","hours_sat","hours_sun","hours_bh","pay_day","pay_night","pay_sat","pay_sun","pay_bh","charge_day","charge_night","charge_sat","charge_sun","charge_bh","total_hours","total_pay_ex_vat","total_charge_ex_vat","margin_ex_vat","processing_status","expenses_pay_ex_vat","expenses_charge_ex_vat","expenses_description","expenses_evidence_manifest","mileage_pay_ex_vat","mileage_charge_ex_vat","mileage_pay_rate","mileage_charge_rate","mileage_evidence_manifest","actual_schedule_json","actual_minutes_by_day_json","additional_units_json","additional_pay_ex_vat","additional_charge_ex_vat","additional_margin_ex_vat","invoice_breakdown_json","nhsp_import_id","has_rate_issue","hr_crosscheck_status","hr_crosscheck_issues","external_source_rows_json","mileage_units","travel_pay_ex_vat","travel_charge_ex_vat","accommodation_pay_ex_vat","accommodation_charge_ex_vat","other_pay_ex_vat","other_charge_ex_vat","stale_reason","pay_method","locked_by_invoice_id","unlocked_by_credit_note_id","po_number","pay_on_hold","pay_on_hold_reason","has_pay_channel_issue"]},
      {"table_name":"timesheet_evidence","bump_generate":true,"bump_issue":true,"fields":["timesheet_id","kind","storage_key","source_revision","display_name","document_asset_id","processing_state","processing_error_json"]},
      {"table_name":"invoice_document_versions","bump_generate":true,"bump_issue":true,"fields":["entity_type","entity_id","purpose","operation_id","source_revision","template_version","status","r2_key","sha256","size_bytes","page_count","ready_at_utc","verified_at_utc","superseded_at_utc","error_json"]},
      {"table_name":"invoice_document_assets","bump_generate":true,"bump_issue":true,"fields":["source_kind","source_id","source_revision","declared_media_type","detected_media_type","original_sha256","original_size_bytes","status","normalised_manifest_hash","normalised_r2_key","normalised_sha256","normalised_size_bytes","normalised_page_count","operation_id","error_json","ready_at_utc"]},
      {"table_name":"invoice_operations","bump_generate":true,"bump_issue":true,"fields":["parent_operation_id","operation_type","entity_type","entity_id","status","phase","source_revision","template_version","control_version","manifest_generation","manifest_committed","release_complete"],"json_paths":["result_json.legal_issue_state","result_json.delivery_state","result_json.document_version_id","result_json.issued_document_version_id","error_json.code"]},
      {"table_name":"invoice_operation_chunks","bump_generate":true,"bump_issue":true,"fields":["operation_id","chunk_type","entity_type","entity_id","document_version_id","document_asset_id","input_document_version_id","status","phase","replaced_by_chunk_id","manifest_generation","is_manifest_member","manifest_committed","result_visible","selection_key","result_category"],"json_paths":["payload_json.is_selection_expander","payload_json.blocked_for_sending","payload_json.row_kind","payload_json.source_revision","result_json.document_version_id","result_json.issued_document_version_id","result_json.legal_issue_state","result_json.delivery_state","error_json.code"]}
    ]
    $manifest$::jsonb) as manifest(
      table_name text,
      bump_generate boolean,
      bump_issue boolean,
      fields jsonb,
      json_paths jsonb
    )
  loop
    v_manifest_rows := v_manifest_rows || jsonb_build_array(
      jsonb_build_object(
        'table_name', v_item.table_name,
        'bump_generate', v_item.bump_generate,
        'bump_issue', v_item.bump_issue,
        'fields', v_item.fields,
        'json_paths', v_item.json_paths
      )
    );

    if to_regclass(format('public.%I', v_item.table_name)) is null then
      raise exception using
        errcode = '42P01',
        message = 'INVOICE_ASYNC_V8_TRIGGER_TABLE_MISSING:' ||
          v_item.table_name;
    end if;

    if exists (
      select 1
      from jsonb_array_elements_text(v_item.fields) field_name
      left join pg_attribute a
        on a.attrelid = format('public.%I', v_item.table_name)::regclass
       and a.attname = field_name.value
       and a.attnum > 0
       and not a.attisdropped
      where a.attname is null
    ) then
      raise exception using
        errcode = '42703',
        message = 'INVOICE_ASYNC_V8_TRIGGER_COLUMN_MISSING:' ||
          v_item.table_name;
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
      v_item.bump_generate::text,
      v_item.bump_issue::text,
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
      v_item.bump_generate::text,
      v_item.bump_issue::text,
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
      v_item.bump_generate::text,
      v_item.bump_issue::text,
      v_argument_list
    );
    v_installed := v_installed + 3;
  end loop;

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

  return jsonb_build_object(
    'contract_version', 'INVOICE_ASYNC_TRIGGER_MANIFEST_V2',
    'candidate_trigger_count', v_installed,
    'result_trigger_count', 3,
    'table_count', v_installed / 3,
    'manifest_digest', private._invoice_batch_hash_v2(v_manifest_rows)
  );
end;
$function$;

alter function private._invoice_candidate_triggers_install_v2()
  owner to postgres;
revoke all on function private._invoice_candidate_triggers_install_v2()
  from public, anon, authenticated;
grant execute on function private._invoice_candidate_triggers_install_v2()
  to service_role;

select private._invoice_candidate_triggers_install_v2();
