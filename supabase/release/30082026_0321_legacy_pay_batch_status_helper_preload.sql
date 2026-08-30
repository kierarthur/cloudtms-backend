-- LEGACY_UPGRADE bootstrap-order preload.
--
-- Historical LIVE contains the current pay-state summary trigger but predates
-- this helper. The earliest rerunnable Banking schema owner normalises
-- pay_batches and therefore fires that trigger before the later canonical
-- monolith recreates the helper. Install the exact current signed definition
-- first, before 08042026_1151_newtablesbanking.sql, and apply the same exact
-- idempotent definition again in the repeatable-preload phase because a later
-- historical migration can replace/remove it before final convergence. This
-- file changes no payment state, financial rule, or Policy X authority.

-- _pay_batch_status_is_active_reservation(text)
CREATE OR REPLACE FUNCTION public._pay_batch_status_is_active_reservation(p_status text)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT upper(btrim(coalesce(p_status, ''))) IN (
    'DRAFT',
    'DRAFT_CREATED',
    'READY',
    'WAITING_BANK_CONFIRM',
    'PARTIAL',
    'BLOCKED_FUNDS',
    'SCHEDULED',
    'EXECUTING',
    'AWAITING_AUTHORISATION',
    'AUTHORISED_FOR_PAYMENT'
  );
$function$;

ALTER FUNCTION public._pay_batch_status_is_active_reservation(text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public._pay_batch_status_is_active_reservation(text) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public._pay_batch_status_is_active_reservation(text) TO service_role;
