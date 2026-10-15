-- Proposed final chronological source module. No installation has been performed.
-- Once-only request migration and the full selected Agency caller prerequisites are mandatory.
begin;
\ir repeatable/02_same_request_identity_guard.sqlinc
\ir repeatable/03_actual_source_phase_admission.sqlinc
\ir repeatable/04_active_source_mutation_guard.sqlinc
\ir repeatable/05_actual_native_capture_and_digest.sqlinc
\ir repeatable/06_actual_original_claim_recheck.sqlinc
\ir repeatable/115_complete_current_request_assert_with_original_claim_recheck.sqlinc
\ir repeatable/07_actual_native_begin.sqlinc
\ir repeatable/116_complete_current_source_raw05_with_active_before_union.sqlinc
\ir repeatable/117_current_source_final_before.sqlinc
\ir repeatable/118_current_source_raw_and_final_76_registrations.sqlinc
notify pgrst, 'reload schema';
commit;
