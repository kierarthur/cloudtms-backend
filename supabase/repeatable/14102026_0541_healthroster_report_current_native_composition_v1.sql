-- Final Report native successors after canonical Money0430.
-- Source presence supplies no native qualification or activation.
\set ON_ERROR_STOP on
begin;
\ir 14102026_0541_healthroster_report_current_native_composition/13_slot_validators.sqlinc
\ir 14102026_0541_healthroster_report_current_native_composition/01_certificate.sqlinc
\ir 14102026_0541_healthroster_report_current_native_composition/02_release_feature_strategy.sqlinc
\ir 14102026_0541_healthroster_report_current_native_composition/03_operational_dispatch.sqlinc
\ir 14102026_0541_healthroster_report_current_native_composition/04_complete_fin3_dispatch.sqlinc
\ir 14102026_0541_healthroster_report_current_native_composition/05_complete_current_feature_setup.sqlinc
\ir 14102026_0541_healthroster_report_current_native_composition/06_complete_fin4_lock_observation.sqlinc
\ir 14102026_0541_healthroster_report_current_native_composition/15_complete_native_report_admission.sqlinc
\ir 14102026_0541_healthroster_report_current_native_composition/14_complete_native_snapshot_assembler.sqlinc
\ir 14102026_0541_healthroster_report_current_native_composition/07_owner_acl.sqlinc
-- Reassert every actual definition in this atomic stage; preserve existing service snapshot authority.
alter function private.weekly_source_report_fin_native_comparisons_assert_v1(jsonb,jsonb,text) owner to postgres;
revoke all on function private.weekly_source_report_fin_native_comparisons_assert_v1(jsonb,jsonb,text) from public,anon,authenticated,service_role;
alter function private.weekly_source_report_fin_qualifications_assert_v1(jsonb,jsonb,text) owner to postgres;
revoke all on function private.weekly_source_report_fin_qualifications_assert_v1(jsonb,jsonb,text) from public,anon,authenticated,service_role;
alter function private.weekly_source_report_fin_certificate_assert_v1(jsonb,uuid,uuid,date,date) owner to postgres;
revoke all on function private.weekly_source_report_fin_certificate_assert_v1(jsonb,uuid,uuid,date,date) from public,anon,authenticated,service_role;
alter function private.weekly_source_report_fin_release_evidence_v1(uuid,uuid,date,date,jsonb) owner to postgres;
revoke all on function private.weekly_source_report_fin_release_evidence_v1(uuid,uuid,date,date,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_report_fin_feature_binding_v1(uuid,uuid,date,date,jsonb) owner to postgres;
revoke all on function private.weekly_source_report_fin_feature_binding_v1(uuid,uuid,date,date,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_report_fin_strategy_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_report_fin_strategy_v1(uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_report_fin_dispatch_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_report_fin_dispatch_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean) owner to postgres;
revoke all on function private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean) from public,anon,authenticated,service_role;
alter function public.weekly_source_group_rule_feature_set_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_group_rule_feature_set_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_group_rule_feature_set_atomic_v1(jsonb) to service_role;
alter function private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean,text) owner to postgres;
revoke all on function private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean,text) from public,anon,authenticated,service_role;
alter function public.weekly_source_invoice_admit_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_invoice_admit_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_invoice_admit_atomic_v1(jsonb) to service_role;
alter function private._invoice_presentation_snapshot_batch(jsonb,timestamp with time zone) owner to postgres;
revoke all on function private._invoice_presentation_snapshot_batch(jsonb,timestamp with time zone) from public,anon,authenticated,service_role;
grant execute on function private._invoice_presentation_snapshot_batch(jsonb,timestamp with time zone) to service_role;
notify pgrst, 'reload schema';
commit;
