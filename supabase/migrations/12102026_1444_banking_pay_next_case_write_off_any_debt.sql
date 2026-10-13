-- FC11 additive nullable-origin extension; preserve installed1647 rows/money/FKs.
\set ON_ERROR_STOP on
begin;
do $migration$
begin
 if (select pg_catalog.pg_get_constraintdef(c.oid) from pg_catalog.pg_constraint c
     where c.conrelid='private.bpay_next_write_off_request'::pg_catalog.regclass
       and c.conname='bpay_next_write_off_request_check')
   is distinct from 'CHECK (((work_collection_id = case_id) AND (work_collection_id = case_component_id)))'
 then raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_ORIGIN_CHECK_NOT_EXACT';end if;
end
$migration$;
alter table private.bpay_next_write_off_request drop constraint bpay_next_write_off_request_check;
alter table private.bpay_next_write_off_request alter column work_collection_id drop not null;
alter table private.bpay_next_write_off_request add constraint bpay_next_write_off_request_origin_identity_v2
 check (work_collection_id is null or (work_collection_id=case_id and work_collection_id=case_component_id));
commit;
