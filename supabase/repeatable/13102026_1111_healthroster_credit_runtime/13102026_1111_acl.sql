-- Final closure ACL: preserve audited existing grants; new private helpers
-- are owner-only and new public RPCs are service-only. Never rely on defaults.
alter function private._invoice_credit_resume_v1(uuid,uuid,uuid,uuid) owner to postgres;
alter function private._invoice_dispatch_advance_batch(jsonb,timestamptz) owner to postgres;
alter function private._invoice_generation_advance_core_v8(jsonb,timestamptz) owner to postgres;
alter function private._invoice_generation_resolve_command_groups(jsonb,uuid,timestamptz) owner to postgres;
alter function private._invoice_operation_start_core_v8(jsonb,uuid,timestamptz) owner to postgres;
alter function private._invoice_support_member_guard_v1() owner to postgres;
alter function public._ctms_assert_invoice_can_unissue_v1(uuid,boolean,text) owner to postgres;
alter function public.invoice_create_credit_note_and_unlock(uuid,uuid) owner to postgres;
revoke all on function
  private._invoice_credit_resume_v1(uuid,uuid,uuid,uuid),
  private._invoice_dispatch_advance_batch(jsonb,timestamptz),
  private._invoice_generation_advance_core_v8(jsonb,timestamptz),
  private._invoice_generation_resolve_command_groups(jsonb,uuid,timestamptz),
  private._invoice_operation_start_core_v8(jsonb,uuid,timestamptz),
  private._invoice_support_member_guard_v1(),
  public._ctms_assert_invoice_can_unissue_v1(uuid,boolean,text),
  public.invoice_create_credit_note_and_unlock(uuid,uuid)
  from public,anon,authenticated,service_role,authenticator,supabase_admin;
grant execute on function
  private._invoice_credit_resume_v1(uuid,uuid,uuid,uuid),
  private._invoice_dispatch_advance_batch(jsonb,timestamptz),
  private._invoice_generation_advance_core_v8(jsonb,timestamptz),
  private._invoice_generation_resolve_command_groups(jsonb,uuid,timestamptz),
  private._invoice_operation_start_core_v8(jsonb,uuid,timestamptz),
  private._invoice_support_member_guard_v1(),
  public._ctms_assert_invoice_can_unissue_v1(uuid,boolean,text),
  public.invoice_create_credit_note_and_unlock(uuid,uuid)
  to postgres;
grant execute on function
  private._invoice_dispatch_advance_batch(jsonb,timestamptz),
  private._invoice_generation_advance_core_v8(jsonb,timestamptz),
  private._invoice_generation_resolve_command_groups(jsonb,uuid,timestamptz),
  private._invoice_operation_start_core_v8(jsonb,uuid,timestamptz),
  public.invoice_create_credit_note_and_unlock(uuid,uuid)
  to service_role;
