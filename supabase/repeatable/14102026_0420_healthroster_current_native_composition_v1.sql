-- Complete current Banking/WP-PP plus HealthRoster native owner composition.
-- WIP: original prerequisites, final ACL registry, contract and native gates remain required.
begin;
\ir 14102026_0420_healthroster_current_native_composition/weekly_exceptional_pay_complete_local_v1.complete.sqlinc
\ir 14102026_0420_healthroster_current_native_composition/weekly_source_manual_review_resolve_v1.complete.sqlinc
\ir 14102026_0420_healthroster_current_native_composition/weekly_exceptional_pay_stage_c1_request_v1.complete.sqlinc
\ir 14102026_0420_healthroster_current_native_composition/weekly_source_candidate_submission_compare_sync_v1.complete.sqlinc
\ir 14102026_0420_healthroster_current_native_composition/weekly_source_manual_reviews_finalised_v1.complete.sqlinc
alter function public.weekly_exceptional_pay_complete_local_v1(jsonb) owner to postgres;
revoke all on function public.weekly_exceptional_pay_complete_local_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_exceptional_pay_complete_local_v1(jsonb) to service_role;
alter function public.weekly_source_manual_review_resolve_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_manual_review_resolve_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_manual_review_resolve_v1(jsonb) to service_role;
alter function public.weekly_exceptional_pay_stage_c1_request_v1(jsonb) owner to postgres;
revoke all on function public.weekly_exceptional_pay_stage_c1_request_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_exceptional_pay_stage_c1_request_v1(jsonb) to service_role;
alter function private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamptz) owner to postgres;
revoke all on function private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamptz) from public,anon,authenticated,service_role;
alter function private.weekly_source_manual_reviews_finalised_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_manual_reviews_finalised_v1(uuid,uuid) from public,anon,authenticated,service_role;
notify pgrst, 'reload schema';
commit;
