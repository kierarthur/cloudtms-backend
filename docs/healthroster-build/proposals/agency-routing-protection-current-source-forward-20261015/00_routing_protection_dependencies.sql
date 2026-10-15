-- After the SAME planned protection store migration and current ffaf runtime.
begin;
\ir forward/132_complete_capture_with_native_agency_admission.sqlinc
\ir forward/133_complete_request_assert_without_source_router_gate.sqlinc
\ir forward/134_complete_current_authorise_router_with_agency_ordinary_route.sqlinc
\ir forward/136_complete_current_protected_match_with_actual_agency_policy.sqlinc
\ir forward/135_same_protection_identity_guard.sqlinc
\ir 137_exact_routing_protection_security.sqlinc
notify pgrst, 'reload schema';
commit;
