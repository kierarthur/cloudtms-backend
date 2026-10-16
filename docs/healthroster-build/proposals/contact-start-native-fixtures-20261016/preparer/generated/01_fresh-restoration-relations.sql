-- Root obtains this fresh under its existing owned turn, before staging.
select coalesce(jsonb_agg(jsonb_build_object('schema',n.nspname,'name',c.relname,'kind',c.relkind::text) order by n.nspname,c.relname),'[]'::jsonb)::text
 from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname in('public','private') and c.relkind in('r','p');
