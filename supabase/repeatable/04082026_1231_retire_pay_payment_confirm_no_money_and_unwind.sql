-- Retain the installed compatibility body unchanged, but make it unreachable
-- by every application role. Confirmed-no-money release is now asynchronous.
ALTER FUNCTION public.pay_payment_confirm_no_money_and_unwind(uuid,jsonb,uuid,jsonb,text,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_confirm_no_money_and_unwind(uuid,jsonb,uuid,jsonb,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_confirm_no_money_and_unwind(uuid,jsonb,uuid,jsonb,text,text) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_confirm_no_money_and_unwind(uuid,jsonb,uuid,jsonb,text,text) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_payment_confirm_no_money_and_unwind(uuid,jsonb,uuid,jsonb,text,text) FROM service_role;
