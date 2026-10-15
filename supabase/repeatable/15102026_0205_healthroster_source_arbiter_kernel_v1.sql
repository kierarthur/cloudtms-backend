-- Source kernel foundation only; full caller/RAW/ACTIVE integration is separate.
begin;
\ir 15102026_0205_healthroster_source_arbiter_kernel/00_source_token_grammar.sqlinc
\ir 15102026_0205_healthroster_source_arbiter_kernel/01_complete_scalar_core.sqlinc
\ir 15102026_0205_healthroster_source_arbiter_kernel/02_complete_coverage_and_dispatch.sqlinc
notify pgrst, 'reload schema';
commit;
