-- Complete native Report reads, internal FIN overload and upload context.
-- Operational A/B dispatch remains held pending real release-bound proof.
\set ON_ERROR_STOP on
begin;
\ir 13102026_2110_healthroster_report_native_fin_worker/01_projection_receipt.sqlinc
\ir 13102026_2110_healthroster_report_native_fin_worker/02_pay_authority.sqlinc
\ir 13102026_2110_healthroster_report_native_fin_worker/03_pay_witness.sqlinc
\ir 13102026_2110_healthroster_report_native_fin_worker/04_complete_fin_overload.sqlinc
\ir 13102026_2110_healthroster_report_native_fin_worker/08_complete_upload_context.sqlinc
\ir 13102026_2110_healthroster_report_native_fin_worker/07_private_acl.sqlinc
notify pgrst, 'reload schema';
commit;
