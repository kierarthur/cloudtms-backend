-- ONE_TIME_ADMISSION only. Never included in a rerunnable repeatable.
-- Two genuinely new Health read helper names must have no previous owner.
do $admission$
declare routine_name text;
begin
 foreach routine_name in array array[
  'weekly_source_pay_decision_native_status_child_v1',
  'weekly_source_pay_decision_read_access_v1'
 ] loop
  if exists(select 1 from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace
   where n.nspname='private' and p.proname=routine_name) then
   raise exception 'SOURCE_CURRENT_READ_NAMESPACE_COLLISION: %',routine_name using errcode='55000';
  end if;
 end loop;
end;
$admission$;
