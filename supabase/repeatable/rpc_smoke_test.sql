create or replace function public.rpc_smoke_test()
returns text
language sql
as $$
  select 'SMOKE_V1'::text;
$$;
