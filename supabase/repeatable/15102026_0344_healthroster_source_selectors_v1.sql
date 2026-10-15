-- Canonical exact Source witness/footprint/raw/nested helper composition.
-- No trigger registration or application admission in this module.
begin;
\ir 15102026_0344_healthroster_source_selectors/06_complete_source_witness_policy_projection.sqlinc
\ir 15102026_0344_healthroster_source_selectors/04_native_source_footprint.sqlinc
\ir 15102026_0344_healthroster_source_selectors/05_raw_selector_helpers.sqlinc
notify pgrst, 'reload schema';
commit;
