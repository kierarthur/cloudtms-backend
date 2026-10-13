-- Final closure ACL: preserve audited existing grants; new private helpers
-- are owner-only and new public RPCs are service-only. Never rely on defaults.
alter function private._invoice_correction_validate_batch(jsonb,date) owner to postgres;
alter function private._invoice_document_advance_batch(jsonb,timestamptz) owner to postgres;
alter function private._invoice_generation_advance_core_v8(jsonb,timestamptz) owner to postgres;
alter function private._invoice_generation_resolve_command_groups(jsonb,uuid,timestamptz) owner to postgres;
alter function private._invoice_operation_start_core_v8(jsonb,uuid,timestamptz) owner to postgres;
alter function private._invoice_presentation_snapshot_batch(jsonb,timestamptz) owner to postgres;
alter function private.invoice_credit_original_input_receipt_v1(jsonb) owner to postgres;
alter function private.invoice_credit_scope_context_v1(uuid,uuid,uuid,uuid,boolean) owner to postgres;
alter function private.invoice_credit_scoped_advance_v1(jsonb,timestamptz) owner to postgres;
alter function private.timesheet_correction_nullable_daily_contract_v1(jsonb,uuid) owner to postgres;
alter function private.timesheet_correction_scoped_source_identity_v1(jsonb) owner to postgres;
alter function public.invoice_create_credit_note_and_unlock(uuid,uuid) owner to postgres;
revoke all on function
  private._invoice_correction_validate_batch(jsonb,date),
  private._invoice_document_advance_batch(jsonb,timestamptz),
  private._invoice_generation_advance_core_v8(jsonb,timestamptz),
  private._invoice_generation_resolve_command_groups(jsonb,uuid,timestamptz),
  private._invoice_operation_start_core_v8(jsonb,uuid,timestamptz),
  private._invoice_presentation_snapshot_batch(jsonb,timestamptz),
  private.invoice_credit_original_input_receipt_v1(jsonb),
  private.invoice_credit_scope_context_v1(uuid,uuid,uuid,uuid,boolean),
  private.invoice_credit_scoped_advance_v1(jsonb,timestamptz),
  private.timesheet_correction_nullable_daily_contract_v1(jsonb,uuid),
  private.timesheet_correction_scoped_source_identity_v1(jsonb),
  public.invoice_create_credit_note_and_unlock(uuid,uuid)
  from public,anon,authenticated,service_role,authenticator,supabase_admin;
grant execute on function
  private._invoice_correction_validate_batch(jsonb,date),
  private._invoice_document_advance_batch(jsonb,timestamptz),
  private._invoice_generation_advance_core_v8(jsonb,timestamptz),
  private._invoice_generation_resolve_command_groups(jsonb,uuid,timestamptz),
  private._invoice_operation_start_core_v8(jsonb,uuid,timestamptz),
  private._invoice_presentation_snapshot_batch(jsonb,timestamptz),
  private.invoice_credit_original_input_receipt_v1(jsonb),
  private.invoice_credit_scope_context_v1(uuid,uuid,uuid,uuid,boolean),
  private.invoice_credit_scoped_advance_v1(jsonb,timestamptz),
  private.timesheet_correction_nullable_daily_contract_v1(jsonb,uuid),
  private.timesheet_correction_scoped_source_identity_v1(jsonb),
  public.invoice_create_credit_note_and_unlock(uuid,uuid)
  to postgres;
grant execute on function
  private._invoice_correction_validate_batch(jsonb,date),
  private._invoice_document_advance_batch(jsonb,timestamptz),
  private._invoice_generation_advance_core_v8(jsonb,timestamptz),
  private._invoice_generation_resolve_command_groups(jsonb,uuid,timestamptz),
  private._invoice_operation_start_core_v8(jsonb,uuid,timestamptz),
  private._invoice_presentation_snapshot_batch(jsonb,timestamptz),
  public.invoice_create_credit_note_and_unlock(uuid,uuid)
  to service_role;
