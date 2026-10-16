-- V6 global timing policy: complete current owners after historical repeatables.
\set ON_ERROR_STOP on
begin;
\ir includes/weekly_contact_timing/010_timing_validation.sqlinc
\ir includes/weekly_contact_timing/030_global_owner.sqlinc
\ir includes/weekly_contact_timing/040_native_timing.sqlinc
\ir 13102026_1438_weekly_source_protection_queries/11_manager_due_scope.sqlinc
\ir 13102026_1438_weekly_source_protection_queries/13_manager_generation.sqlinc
\ir 13102026_1438_weekly_source_protection_queries/14_not_finalised_dispatch.sqlinc
\ir includes/weekly_contact_timing/070_acl.sqlinc
notify pgrst, 'reload schema';
commit;
