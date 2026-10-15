-- Canonical source union, not yet qualified for release.
begin;
\ir 15102026_0216_healthroster_a0_physical_source_union/01_physical_token_and_retention_helpers.sqlinc
\ir 15102026_0216_healthroster_a0_physical_source_union/05_native_creator_intents_and_triggers.sqlinc
\ir 15102026_0216_healthroster_a0_physical_source_union/06_send_family_fence.sqlinc
\ir 15102026_0216_healthroster_a0_physical_source_union/08_complete_current_request.sqlinc
\ir 15102026_0216_healthroster_a0_physical_source_union/04_complete_native_lock_fence.sqlinc
\ir 15102026_0216_healthroster_a0_physical_source_union/09_native_child_raw_intents.sqlinc
alter function private.timesheet_correction_canonical_input_assert_v1(jsonb,uuid,integer,uuid,uuid) owner to postgres;
revoke all on function private.timesheet_correction_canonical_input_assert_v1(jsonb,uuid,integer,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_clock_scope_v1(uuid) owner to postgres;
revoke all on function private.timesheet_correction_a0_clock_scope_v1(uuid) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_native_input_witness_v1(uuid) owner to postgres;
revoke all on function private.timesheet_correction_a0_native_input_witness_v1(uuid) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_native_lock_fence_v1(uuid,uuid) owner to postgres;
revoke all on function private.timesheet_correction_a0_native_lock_fence_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_signed_precursor_v1(uuid) owner to postgres;
revoke all on function private.timesheet_correction_a0_signed_precursor_v1(uuid) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_writer_admit_v1(uuid[],text[],text) owner to postgres;
revoke all on function private.timesheet_correction_a0_writer_admit_v1(uuid[],text[],text) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_reference_before_v1() owner to postgres;
revoke all on function private.timesheet_correction_a0_reference_before_v1() from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_booking_before_v1() owner to postgres;
revoke all on function private.timesheet_correction_a0_booking_before_v1() from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_candidate_before_v1() owner to postgres;
revoke all on function private.timesheet_correction_a0_candidate_before_v1() from public,anon,authenticated,service_role;
alter function private.timesheet_correction_a0_send_family_fence_v1(uuid) owner to postgres;
revoke all on function private.timesheet_correction_a0_send_family_fence_v1(uuid) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_component_negate_v1(jsonb,text[]) owner to postgres;
revoke all on function private.timesheet_correction_component_negate_v1(jsonb,text[]) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_native_original_reversal_v1(jsonb,uuid) owner to postgres;
revoke all on function private.timesheet_correction_native_original_reversal_v1(jsonb,uuid) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_snapshot_identity_assert_v1(jsonb,uuid,integer,uuid,uuid) owner to postgres;
revoke all on function private.timesheet_correction_snapshot_identity_assert_v1(jsonb,uuid,integer,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_reversal_component_assert_v1(jsonb,jsonb,text[]) owner to postgres;
revoke all on function private.timesheet_correction_reversal_component_assert_v1(jsonb,jsonb,text[]) from public,anon,authenticated,service_role;
alter function private.timesheet_correction_reversal_assert_v1(jsonb,jsonb) owner to postgres;
revoke all on function private.timesheet_correction_reversal_assert_v1(jsonb,jsonb) from public,anon,authenticated,service_role;
alter function public.timesheet_correction_prepare_v1(uuid,uuid) owner to postgres;
revoke all on function public.timesheet_correction_prepare_v1(uuid,uuid) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_correction_prepare_v1(uuid,uuid) to service_role;
alter function public.timesheet_correction_authorise_atomic_v1(uuid,bigint,jsonb,uuid,timestamp with time zone) owner to postgres;
revoke all on function public.timesheet_correction_authorise_atomic_v1(uuid,bigint,jsonb,uuid,timestamp with time zone) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_correction_authorise_atomic_v1(uuid,bigint,jsonb,uuid,timestamp with time zone) to service_role;
alter function private.timesheet_correction_week_allocate_v1(uuid,date,public.submission_mode_enum,uuid,timestamp with time zone) owner to postgres;
revoke all on function private.timesheet_correction_week_allocate_v1(uuid,date,public.submission_mode_enum,uuid,timestamp with time zone) from public,anon,authenticated,service_role;
alter function public.timesheet_correction_workflow_projection_v1(uuid,uuid) owner to postgres;
revoke all on function public.timesheet_correction_workflow_projection_v1(uuid,uuid) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_correction_workflow_projection_v1(uuid,uuid) to service_role;
alter function private.timesheet_correction_leg_create_v1(private.timesheet_correction_requests,text,jsonb,timestamp with time zone) owner to postgres;
revoke all on function private.timesheet_correction_leg_create_v1(private.timesheet_correction_requests,text,jsonb,timestamp with time zone) from public,anon,authenticated,service_role;
alter function public.timesheet_correction_send_v1(jsonb,uuid,timestamp with time zone) owner to postgres;
revoke all on function public.timesheet_correction_send_v1(jsonb,uuid,timestamp with time zone) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_correction_send_v1(jsonb,uuid,timestamp with time zone) to service_role;
alter function private.timesheet_correction_projection_by_request_v1(uuid,uuid) owner to postgres;
revoke all on function private.timesheet_correction_projection_by_request_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function public.timesheet_correction_request_projection_v1(uuid,uuid) owner to postgres;
revoke all on function public.timesheet_correction_request_projection_v1(uuid,uuid) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_correction_request_projection_v1(uuid,uuid) to service_role;
alter function public.timesheet_correction_create_projection_v1(uuid,uuid,uuid,uuid) owner to postgres;
revoke all on function public.timesheet_correction_create_projection_v1(uuid,uuid,uuid,uuid) from public,anon,authenticated,service_role;
grant execute on function public.timesheet_correction_create_projection_v1(uuid,uuid,uuid,uuid) to service_role;
notify pgrst, 'reload schema';
commit;
