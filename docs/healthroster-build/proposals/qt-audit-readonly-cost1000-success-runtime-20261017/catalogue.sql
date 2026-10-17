with expected_columns(ord,name,type_oid,identity) as(values @COLUMNS@),
expected_types(oid,send_symbol,out_symbol) as(values @TYPES@),
expected_functions(identity,source_symbol,arg_oid,return_oid) as(values @FUNCTIONS@),
r as(select c.* from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname='audit_events')
select
 (select count(*)=1 and coalesce(bool_and(r.relkind='r' and r.relpersistence='p' and r.relrowsecurity and not r.relforcerowsecurity and r.relowner=(select oid from pg_catalog.pg_roles where rolname='dee50tht') and r.relam=(select oid from pg_catalog.pg_am where amname='heap' and amtype='t')),false) from r)
 and (select count(*)=16 from pg_catalog.pg_attribute a join r on r.oid=a.attrelid where a.attnum>0 and not a.attisdropped)
 and not exists(select 1 from expected_columns e where not exists(select 1 from pg_catalog.pg_attribute a join r on r.oid=a.attrelid where a.attnum=e.ord and a.attname=e.name and a.atttypid=e.type_oid and not a.attisdropped and a.attidentity::text=e.identity and a.attgenerated=''))
 and not exists(select 1 from pg_catalog.pg_inherits i join r on r.oid in(i.inhrelid,i.inhparent))
 and not exists(select 1 from pg_catalog.pg_rewrite w join r on r.oid=w.ev_class)
 and (select count(*)=1 from pg_catalog.pg_policy p join r on r.oid=p.polrelid)
 and (select count(*)=2 from pg_catalog.pg_roles where rolname in('dee50tht','service_role'))
 and exists(select 1 from pg_catalog.pg_policy p join r on r.oid=p.polrelid where p.polname='cloudtms_miget_service_owner_all' and p.polcmd='*' and p.polpermissive and pg_catalog.pg_get_expr(p.polqual,p.polrelid)='true' and pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid)='true'
 and array(select unnest(p.polroles) order by 1)=(select array_agg(oid order by oid) from pg_catalog.pg_roles where rolname in('dee50tht','service_role')))
 and not exists(select 1 from expected_types e where not exists(select 1 from pg_catalog.pg_type t join pg_catalog.pg_namespace n on n.oid=t.typnamespace join pg_catalog.pg_proc snd on snd.oid=t.typsend join pg_catalog.pg_language sl on sl.oid=snd.prolang join pg_catalog.pg_proc outp on outp.oid=t.typoutput join pg_catalog.pg_language ol on ol.oid=outp.prolang
 where t.oid=e.oid and n.nspname='pg_catalog' and t.typtype='b' and t.typbasetype=0 and t.typisdefined
 and snd.pronargs=1 and snd.proargtypes[0]=t.oid and snd.prorettype=17 and not snd.proretset and snd.provariadic=0 and outp.pronargs=1 and outp.proargtypes[0]=t.oid and outp.prorettype=2275 and not outp.proretset and outp.provariadic=0
 and snd.pronamespace=n.oid and snd.proowner=10 and snd.prokind='f' and sl.lanname='internal' and snd.prosrc=e.send_symbol and snd.probin is null and snd.proconfig is null and not snd.prosecdef and snd.provolatile in('i','s')
 and outp.pronamespace=n.oid and outp.proowner=10 and outp.prokind='f' and ol.lanname='internal' and outp.prosrc=e.out_symbol and outp.probin is null and outp.proconfig is null and not outp.prosecdef and outp.provolatile in('i','s')))
 and not exists(select 1 from expected_functions e where not exists(select 1 from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace join pg_catalog.pg_language l on l.oid=p.prolang
 where p.oid=pg_catalog.to_regprocedure(e.identity) and p.pronargs=1 and p.proargtypes[0]=e.arg_oid and p.prorettype=e.return_oid and not p.proretset and p.provariadic=0 and n.nspname='pg_catalog' and p.proowner=10 and p.prokind='f' and l.lanname='internal' and p.prosrc=e.source_symbol and p.probin is null and p.proconfig is null and not p.prosecdef and p.provolatile in('i','s'))) as allowed