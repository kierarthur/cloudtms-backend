\set ON_ERROR_STOP on
begin;
\ir 17102026_0135_correction_entry_reader/01_complete_entry_reader.sqlinc
\ir 17102026_0135_correction_entry_reader/98_complete_current_classification.sqlinc
notify pgrst,'reload schema';
commit;
