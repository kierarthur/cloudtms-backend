CREATE OR REPLACE FUNCTION public.trg_set_cli_ref()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  IF TG_OP = 'INSERT' THEN
    -- Always mint a fresh CLI code on create
    NEW.cli_ref := 'CLI-' || to_char(nextval('public.client_cliref_seq'), 'FM00000');
  ELSIF TG_OP = 'UPDATE' THEN
    -- Immutable after creation
    IF NEW.cli_ref IS DISTINCT FROM OLD.cli_ref THEN
      RAISE EXCEPTION 'cli_ref is immutable once assigned';
    END IF;
  END IF;
  RETURN NEW;
END;
$function$;