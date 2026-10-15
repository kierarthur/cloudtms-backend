-- One complete current Agency runtime after mandatory once-only schemas and foundations.
begin;
\ir 01_complete_current_agency_runtime.sqlinc
notify pgrst, 'reload schema';
commit;
