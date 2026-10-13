-- Complete native lineage successor: genuine own-created LEGACY root only.
\set ON_ERROR_STOP on
begin;
\ir 13102026_2025_healthroster_lineage_own_created_root/complete_lineage_owner.sqlinc
notify pgrst, 'reload schema';
commit;
