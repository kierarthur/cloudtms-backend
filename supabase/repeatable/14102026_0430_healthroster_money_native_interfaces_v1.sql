-- Complete corrected Money interfaces, preserving current Bank/WP owners.
-- No qualification or activation follows from source presence.
begin;
\ir 14102026_0430_healthroster_money_native_interfaces/01_OFF_RULE_AUTHORITY.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/02_HISTORICAL_FIXTURE_ASSIGNMENT.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/03_HISTORICAL_FIXTURE_SETUP_ASSERT.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/04_13_complete_nullable_physical_qualification_scope.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/05_OFF_RULE_NATIVE_PARENT_SCOPE.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/06_OFF_RULE_COMMAND.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/07_OFF_RULE_STATUS.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/08_OFF_RULE_TRANSPORT_STATUS.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/09_01_operational_rule_projection.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/10_02_effective_rule_owner.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/11_03_cas_operational_projection.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/12_04_final_revision_rule_guard.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/13_05_coverage_floor_owner.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/14_07_automatic_capability_admission.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/15_08_cas_admission_owner.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/16_10_complete_bulk_test_scope.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/17_ACTIVATION_WINDOW_BINDING.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/18_FINAL_OPERATIONAL_CERTIFICATE.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/19_ACTIVATION_PROBE_AUTHORIZATION_ASSERT.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/20_ACTIVATION_PROBE_SCOPE.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/21_SETUP_STATIC_SHAPE_ASSERT.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/22_POST_VERIFIED_SETUP_ASSERT.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/23_POST_VERIFIED_BULK_SETUP_ASSERT.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/24_POST_VERIFIED_PARTIAL_ASSERT.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/25_REPORT_FIXTURE_REFERENCE.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/26_NONOPERATIONAL_PREREQUISITE_READER.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/27_POST_VERIFIED_EVIDENCE_RECORD.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/28_12_complete_report_nullable_rounding_owner.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/29_11_complete_historical_fixture_native_assignment.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/30_09_complete_controlled_setup_transport.sqlinc
\ir 14102026_0430_healthroster_money_native_interfaces/31_OWNER_ONLY_ADDITIONS_ACL.sqlinc
alter function private.weekly_source_rule_qualification_authorization_v1(uuid,uuid,text) owner to postgres;
revoke all on function private.weekly_source_rule_qualification_authorization_v1(uuid,uuid,text) from public,anon,authenticated,service_role;
alter function private.weekly_source_rule_qualification_scope_assert_v1(jsonb,uuid,boolean) owner to postgres;
revoke all on function private.weekly_source_rule_qualification_scope_assert_v1(jsonb,uuid,boolean) from public,anon,authenticated,service_role;
alter function private.weekly_source_rule_qualification_context_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_rule_qualification_context_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_historical_fixture_assignment_scope_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_historical_fixture_assignment_scope_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_historical_fixture_assignment_assert_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_historical_fixture_assignment_assert_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_historical_fixture_rule_assign_v1(uuid,jsonb) owner to postgres;
revoke all on function private.weekly_source_historical_fixture_rule_assign_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_historical_fixture_setup_assert_v1(jsonb,boolean) owner to postgres;
revoke all on function private.weekly_source_historical_fixture_setup_assert_v1(jsonb,boolean) from public,anon,authenticated,service_role;
alter function private.weekly_source_protected_qualification_owner_v1() owner to postgres;
revoke all on function private.weekly_source_protected_qualification_owner_v1() from public,anon,authenticated,service_role;
alter function private.weekly_source_protected_qualification_release_v1() owner to postgres;
revoke all on function private.weekly_source_protected_qualification_release_v1() from public,anon,authenticated,service_role;
alter function private.weekly_source_protected_qualification_census_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_protected_qualification_census_v1(uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_protected_qualification_begin_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_protected_qualification_begin_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_protected_qualification_assert_v1(uuid,uuid,uuid[],text) owner to postgres;
revoke all on function private.weekly_source_protected_qualification_assert_v1(uuid,uuid,uuid[],text) from public,anon,authenticated,service_role;
alter function private.weekly_source_off_rule_native_parent_scope_v1(jsonb,uuid,jsonb) owner to postgres;
revoke all on function private.weekly_source_off_rule_native_parent_scope_v1(jsonb,uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_rule_qualification_setup_atomic_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_rule_qualification_setup_atomic_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_rule_qualification_execute_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_rule_qualification_execute_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_rule_qualification_status_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_rule_qualification_status_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_rule_qualification_transport_status_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_rule_qualification_transport_status_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_operational_effective_rule_set_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_operational_effective_rule_set_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_effective_rule_set_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_effective_rule_set_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_bulk_source_facts_v1(uuid,uuid[]) owner to postgres;
revoke all on function private.weekly_source_bulk_source_facts_v1(uuid,uuid[]) from public,anon,authenticated,service_role;
alter function private.weekly_source_bulk_source_cas_v1(uuid,uuid[]) owner to postgres;
revoke all on function private.weekly_source_bulk_source_cas_v1(uuid,uuid[]) from public,anon,authenticated,service_role;
alter function private.weekly_source_final_rule_version_guard_v1() owner to postgres;
revoke all on function private.weekly_source_final_rule_version_guard_v1() from public,anon,authenticated,service_role;
alter function private.weekly_source_rule_coverage_floor_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_rule_coverage_floor_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_automatic_reconcile_capability_assert_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_automatic_reconcile_capability_assert_v1(uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_bulk_source_cas_assert_v1(uuid,uuid[]) owner to postgres;
revoke all on function private.weekly_source_bulk_source_cas_assert_v1(uuid,uuid[]) from public,anon,authenticated,service_role;
alter function private.weekly_source_protected_test_authorization_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_protected_test_authorization_v1(uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_protected_test_scope_assert_v1(uuid,uuid,uuid[],uuid) owner to postgres;
revoke all on function private.weekly_source_protected_test_scope_assert_v1(uuid,uuid,uuid[],uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_protected_test_setup_atomic_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_protected_test_setup_atomic_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_activation_window_binding_assert_v1(jsonb,jsonb,jsonb,uuid,date,date) owner to postgres;
revoke all on function private.weekly_source_activation_window_binding_assert_v1(jsonb,jsonb,jsonb,uuid,date,date) from public,anon,authenticated,service_role;
alter function private.weekly_source_protected_operational_certificate_assert_v1(jsonb,uuid,date,date,text) owner to postgres;
revoke all on function private.weekly_source_protected_operational_certificate_assert_v1(jsonb,uuid,date,date,text) from public,anon,authenticated,service_role;
alter function private.weekly_source_protected_execution_tree_assert_v2(jsonb,text,text) owner to postgres;
revoke all on function private.weekly_source_protected_execution_tree_assert_v2(jsonb,text,text) from public,anon,authenticated,service_role;
alter function private.weekly_source_protected_activation_probe_authorization_assert_v1(jsonb,uuid,date,date,text) owner to postgres;
revoke all on function private.weekly_source_protected_activation_probe_authorization_assert_v1(jsonb,uuid,date,date,text) from public,anon,authenticated,service_role;
alter function private.weekly_source_protected_activation_probe_begin_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_protected_activation_probe_begin_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_protected_activation_probe_assert_v1(uuid,uuid,uuid,date,date,uuid) owner to postgres;
revoke all on function private.weekly_source_protected_activation_probe_assert_v1(uuid,uuid,uuid,date,date,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_protected_activation_probe_execute_v1(uuid,jsonb) owner to postgres;
revoke all on function private.weekly_source_protected_activation_probe_execute_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_off_rule_setup_shape_assert_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_off_rule_setup_shape_assert_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_post_verified_setup_artifact_assert_v1(jsonb,jsonb) owner to postgres;
revoke all on function private.weekly_source_post_verified_setup_artifact_assert_v1(jsonb,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_post_verified_bulk_setup_artifact_assert_v1(jsonb,jsonb) owner to postgres;
revoke all on function private.weekly_source_post_verified_bulk_setup_artifact_assert_v1(jsonb,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_post_verified_partial_artifact_assert_v1(jsonb,jsonb) owner to postgres;
revoke all on function private.weekly_source_post_verified_partial_artifact_assert_v1(jsonb,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_rule_report_fixture_bind_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_rule_report_fixture_bind_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_rule_report_fixture_finalise_v1(jsonb,jsonb) owner to postgres;
revoke all on function private.weekly_source_rule_report_fixture_finalise_v1(jsonb,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_protected_prerequisite_release_assert_v2(jsonb,uuid,date,date,text) owner to postgres;
revoke all on function private.weekly_source_protected_prerequisite_release_assert_v2(jsonb,uuid,date,date,text) from public,anon,authenticated,service_role;
alter function private.weekly_source_post_verified_artifact_assert_v1(text,jsonb,jsonb,text) owner to postgres;
revoke all on function private.weekly_source_post_verified_artifact_assert_v1(text,jsonb,jsonb,text) from public,anon,authenticated,service_role;
alter function private.weekly_source_post_verified_evidence_record_v1(uuid,jsonb) owner to postgres;
revoke all on function private.weekly_source_post_verified_evidence_record_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.weekly_source_vat_rounding_movement_insert_v1(uuid,uuid,uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_vat_rounding_movement_insert_v1(uuid,uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function public.weekly_source_group_rule_assign_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_group_rule_assign_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_group_rule_assign_atomic_v1(jsonb) to service_role;
alter function public.weekly_source_group_rule_feature_set_atomic_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_group_rule_feature_set_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_group_rule_feature_set_atomic_v1(jsonb) to service_role;
notify pgrst, 'reload schema';
commit;
