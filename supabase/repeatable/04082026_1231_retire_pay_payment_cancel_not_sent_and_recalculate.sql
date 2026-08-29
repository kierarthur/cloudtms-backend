-- Retain the installed compatibility body unchanged, but remove it from every
-- application execution role. The asynchronous correction operation is the
-- only normal cancellation owner.
ALTER FUNCTION public.pay_payment_cancel_not_sent_and_recalculate(uuid,jsonb,uuid,text,text,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_cancel_not_sent_and_recalculate(uuid,jsonb,uuid,text,text,jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_cancel_not_sent_and_recalculate(uuid,jsonb,uuid,text,text,jsonb) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_cancel_not_sent_and_recalculate(uuid,jsonb,uuid,text,text,jsonb) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_payment_cancel_not_sent_and_recalculate(uuid,jsonb,uuid,text,text,jsonb) FROM service_role;
