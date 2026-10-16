-- Actual Source logical membership is recorded once; device transports remain native.
-- Reviewed proposal3b6adeb3f4db02cf35d62a887fed88af54a51212. No schema or sender.
\set ON_ERROR_STOP on
begin;
\ir includes/weekly_contact_source_memberships/010_native_logical_members.sqlinc
\ir includes/weekly_contact_source_memberships/020_native_budget.sqlinc
\ir includes/weekly_contact_source_memberships/030_native_enrol_assert.sqlinc
\ir includes/weekly_contact_source_memberships/040_native_accept.sqlinc
\ir includes/weekly_contact_source_memberships/090_acl.sqlinc
notify pgrst,'reload schema';
commit;
