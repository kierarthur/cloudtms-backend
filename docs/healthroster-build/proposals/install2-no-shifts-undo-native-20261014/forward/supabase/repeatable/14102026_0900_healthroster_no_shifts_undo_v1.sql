\set ON_ERROR_STOP on
begin;
\ir 14102026_0900_healthroster_no_shifts_undo/01_no_shifts_undo_atomic.sqlinc
notify pgrst, 'reload schema';
commit;
