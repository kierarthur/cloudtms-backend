CREATE OR REPLACE FUNCTION public._change_bump(p_entity_key text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
begin
  if p_entity_key is null or btrim(p_entity_key) = '' then
    return;
  end if;

  update public.app_change_counters c
     set seq = c.seq + 1,
         updated_at = now()
   where c.entity_key = p_entity_key;

  if not found then
    insert into public.app_change_counters(entity_key, seq, updated_at)
    values (p_entity_key, 1, now())
    on conflict (entity_key) do update
      set seq = public.app_change_counters.seq + 1,
          updated_at = now();
  end if;
end;
$function$;