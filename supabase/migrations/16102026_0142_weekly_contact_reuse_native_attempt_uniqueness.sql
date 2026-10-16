-- One-time CloudTMS schema/data migration: weekly_contact_reuse_native_attempt_uniqueness
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- Forward correction of the preceding, still-uninstalled checkpoint. The
-- base table already has the stronger full UNIQUE(provider_attempt_id).
-- Keep both locked migrations immutable; remove only our redundant index
-- after proving the original authoritative uniqueness remains valid.
do $guard$
declare v_table oid:='public.weekly_contact_submission_permits'::regclass;
 v_index oid:='public.weekly_contact_submission_permits_native_attempt_uq'::regclass;
 v_att smallint;
begin
 select attnum into strict v_att from pg_attribute
 where attrelid=v_table and attname='provider_attempt_id' and not attisdropped;
 if not exists(select 1 from pg_constraint c join pg_index i on i.indexrelid=c.conindid
  where c.conrelid=v_table and c.conname='weekly_contact_submission_permits_provider_attempt_id_key'
   and c.contype='u' and c.convalidated and c.conkey=array[v_att]
   and i.indisunique and i.indisvalid and i.indisready and i.indpred is null)
  or not exists(select 1 from pg_index i where i.indexrelid=v_index and i.indrelid=v_table
   and i.indisunique and i.indisvalid and i.indisready and i.indnkeyatts=1 and i.indnatts=1
   and i.indkey[0]=v_att and pg_get_expr(i.indpred,i.indrelid)='(provider_attempt_id IS NOT NULL)') then
  raise exception 'CONTACT_NATIVE_ATTEMPT_UNIQUENESS_NOT_PROVED';end if;
 execute 'drop index public.weekly_contact_submission_permits_native_attempt_uq';
end;
$guard$;

commit;
