-- Complete native query contact lifecycle and original owner compatibility.
-- One-time trigger installation belongs solely to the atomic0339 migration.
\set ON_ERROR_STOP on
begin;
\ir includes/weekly_contact_query/020_native_member.sqlinc
\ir includes/weekly_contact_query/021_native_payload.sqlinc
\ir includes/weekly_contact_query/030_native_enqueue_core.sqlinc
\ir includes/weekly_contact_query/040_native_recomposition.sqlinc
\ir includes/weekly_contact_query/050_native_entrypoints.sqlinc
\ir includes/weekly_contact_query/060_native_result.sqlinc
\ir includes/weekly_contact_query/065_native_followup.sqlinc
\ir includes/weekly_contact_query/070_native_mark.sqlinc
\ir includes/weekly_contact_query/080_native_integrity.sqlinc
\ir includes/weekly_contact_query/081_native_claim_guard.sqlinc
\ir includes/weekly_contact_query/082_native_claim_kernel.sqlinc
\ir includes/weekly_contact_query/083_native_claim_bridge.sqlinc
\ir includes/weekly_contact_query/086_pause_inflight.sqlinc
\ir includes/weekly_contact_query/090_acl.sqlinc
\ir includes/weekly_contact_query/091_claim_acl.sqlinc
notify pgrst,'reload schema';
commit;
