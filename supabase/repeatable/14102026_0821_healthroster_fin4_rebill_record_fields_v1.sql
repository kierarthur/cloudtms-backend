-- Complete reviewed FIN4 successor; four record-field accesses only.
-- Source packaging is not compiled or native qualification.
\set ON_ERROR_STOP on
begin;
\ir 14102026_0821_healthroster_fin4_rebill_record_fields/01_complete_fin4.sqlinc
alter function private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean,text) owner to postgres;
revoke all on function private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean,text) from public,anon,authenticated,service_role;
notify pgrst, 'reload schema';
commit;
