CREATE OR REPLACE FUNCTION public._trg_change_bump()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_entity_key text;
begin
  v_entity_key := nullif(btrim(coalesce(tg_argv[0], '')), '');

  perform public._change_bump(v_entity_key);

  if tg_op = 'DELETE' then
    return old;
  else
    return new;
  end if;
end;
$function$;