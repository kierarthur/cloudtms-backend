CREATE OR REPLACE FUNCTION public.trg_set_tms_ref()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  IF TG_OP = 'INSERT' THEN
    -- Always mint a fresh CCR code on create
    NEW.tms_ref := 'CCR-' || to_char(nextval('public.candidate_tmsref_seq'), 'FM00000');
  ELSIF TG_OP = 'UPDATE' THEN
    -- Immutable after creation
    IF NEW.tms_ref IS DISTINCT FROM OLD.tms_ref THEN
      RAISE EXCEPTION 'tms_ref is immutable once assigned';
    END IF;
  END IF;
  RETURN NEW;
END;
$function$;