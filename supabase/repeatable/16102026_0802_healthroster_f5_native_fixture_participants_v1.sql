-- HealthRoster protected native TEST fixture constructors and finite scope successors.
-- Exact b287ef0c source; post-VERIFIED only. No schema/table/public operation.
\set ON_ERROR_STOP on

begin;

\ir 16102026_0802_healthroster_f5_native_fixture_participants/001_native_participants.sqlinc
\ir 16102026_0802_healthroster_f5_native_fixture_participants/002_source_metadata.sqlinc
\ir 16102026_0802_healthroster_f5_native_fixture_participants/010_off_rule_scope.sqlinc
\ir 16102026_0802_healthroster_f5_native_fixture_participants/020_historical_assignment.sqlinc
\ir 16102026_0802_healthroster_f5_native_fixture_participants/030_off_rule_shape.sqlinc
\ir 16102026_0802_healthroster_f5_native_fixture_participants/040_post_verified_record.sqlinc

notify pgrst,'reload schema';
commit;
