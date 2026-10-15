do $vacancy$ begin if to_regclass('private.hr_k1978_native_20261015_450040bab9a9518c') is not null or to_regprocedure('private.hr_k1978_native_20261015_450040bab9a9518c_fn()') is not null then raise exception 'KIER1978_NATIVE_PROBE_NOT_VACANT';end if;end $vacancy$;
create table private.hr_k1978_native_20261015_450040bab9a9518c(id integer primary key,probe_value integer not null);
insert into private.hr_k1978_native_20261015_450040bab9a9518c values(1,7);
create function private.hr_k1978_native_20261015_450040bab9a9518c_fn() returns integer language sql immutable set search_path='' as $probe$ select 7 $probe$;
revoke all on function private.hr_k1978_native_20261015_450040bab9a9518c_fn() from public,anon,authenticated,service_role;
do $assert$ begin if (select count(*) from private.hr_k1978_native_20261015_450040bab9a9518c where id=1 and probe_value=private.hr_k1978_native_20261015_450040bab9a9518c_fn())<>1 then raise exception 'KIER1978_NATIVE_PROBE_FAILED';end if;end $assert$;
select current_database() as physical_database,true as temporary_table_and_function_exercised;
