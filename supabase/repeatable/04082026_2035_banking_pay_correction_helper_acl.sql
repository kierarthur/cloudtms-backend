-- CloudTMS Banking Pay cancellation — Stage 1 security boundary.
-- Internal SECURITY DEFINER helpers remain executable only by their postgres
-- owner.  The two diagnostic facades are service-role only.

ALTER FUNCTION public._pay_payment_correction_apply_accepted_finance_resolution(uuid,uuid,uuid)
    OWNER TO postgres;
REVOKE ALL ON FUNCTION public._pay_payment_correction_apply_accepted_finance_resolution(uuid,uuid,uuid)
    FROM PUBLIC, anon, authenticated, service_role;

ALTER FUNCTION public._pay_payment_correction_mail_scope_match(uuid,uuid,jsonb,jsonb,boolean)
    OWNER TO postgres;
REVOKE ALL ON FUNCTION public._pay_payment_correction_mail_scope_match(uuid,uuid,jsonb,jsonb,boolean)
    FROM PUBLIC, anon, authenticated, service_role;

ALTER FUNCTION public._pay_payment_correction_selected_items(uuid,jsonb,boolean)
    OWNER TO postgres;
REVOKE ALL ON FUNCTION public._pay_payment_correction_selected_items(uuid,jsonb,boolean)
    FROM PUBLIC, anon, authenticated, service_role;

ALTER FUNCTION public._pay_payment_correction_validate_accepted_finance_resolution(uuid,jsonb,jsonb,jsonb,uuid)
    OWNER TO postgres;
REVOKE ALL ON FUNCTION public._pay_payment_correction_validate_accepted_finance_resolution(uuid,jsonb,jsonb,jsonb,uuid)
    FROM PUBLIC, anon, authenticated, service_role;

ALTER FUNCTION public.pay_payment_cancelability_diagnostic(uuid,jsonb,uuid,text)
    OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_cancelability_diagnostic(uuid,jsonb,uuid,text)
    FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_cancelability_diagnostic(uuid,jsonb,uuid,text)
    TO service_role;

ALTER FUNCTION public.pay_payment_correction_plan(uuid,jsonb,uuid,text)
    OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_correction_plan(uuid,jsonb,uuid,text)
    FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_payment_correction_plan(uuid,jsonb,uuid,text)
    TO service_role;
