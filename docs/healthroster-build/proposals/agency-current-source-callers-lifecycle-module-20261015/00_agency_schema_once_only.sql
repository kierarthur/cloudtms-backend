-- Once-only managed migrations; never place these DDL bodies in repeatables.
begin;
\ir migration/01_same_agency_request_store.sqlinc
\ir migration/02_typed_existing_validation_columns.sqlinc
commit;
