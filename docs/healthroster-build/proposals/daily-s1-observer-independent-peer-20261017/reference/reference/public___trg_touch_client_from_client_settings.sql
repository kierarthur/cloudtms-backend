CREATE OR REPLACE FUNCTION public._trg_touch_client_from_client_settings()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_client_id uuid;
begin
  v_client_id := coalesce(new.client_id, old.client_id);

  if v_client_id is not null then
    update public.clients cl
       set rev = coalesce(cl.rev, 0) + 1,
           updated_at = now()
     where cl.id = v_client_id;
  end if;

  if tg_op = 'DELETE' then
    return old;
  else
    return new;
  end if;
end;
$function$;