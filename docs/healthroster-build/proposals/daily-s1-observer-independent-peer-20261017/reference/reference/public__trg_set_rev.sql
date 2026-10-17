CREATE OR REPLACE FUNCTION public.trg_set_rev()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  NEW.rev := nextval('public.global_rev_seq');
  RETURN NEW;
END;
$function$;