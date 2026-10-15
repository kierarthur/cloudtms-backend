-- Existing canonical function signatures and ACLs are retained by CREATE OR REPLACE.
-- Only new OFF helper96 carries its reviewed explicit owner/ACL.
begin;
\ir lifecycle/93_complete_fixed_contract_approval_normaliser.sqlinc
\ir lifecycle/94_complete_effective_contract_fixed_approval.sqlinc
\ir lifecycle/95_complete_public_manager_writer_fixed_policy_guard.sqlinc
\ir lifecycle/96_off_agency_native_parent.sqlinc
\ir lifecycle/105_complete_client_preset_fixed_approval.sqlinc
\ir lifecycle/106_complete_feature_setup_fixed_approval_receipts.sqlinc
\ir lifecycle/107_complete_dated_candidate_fixed_approval_policy.sqlinc
\ir lifecycle/108_complete_worker_only_candidate_transition.sqlinc
\ir lifecycle/109_complete_worker_only_component_render_contract.sqlinc
\ir lifecycle/110_complete_worker_only_candidate_finaliser.sqlinc
notify pgrst, 'reload schema';
commit;
