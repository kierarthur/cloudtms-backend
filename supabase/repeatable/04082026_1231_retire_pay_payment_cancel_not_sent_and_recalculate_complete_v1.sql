-- Retain the installed loop-to-completion compatibility body unchanged, but
-- make it unreachable by every application role and normal caller.
ALTER FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_complete_v1(uuid,jsonb,uuid,text,text,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_complete_v1(uuid,jsonb,uuid,text,text,jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_complete_v1(uuid,jsonb,uuid,text,text,jsonb) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_complete_v1(uuid,jsonb,uuid,text,text,jsonb) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_complete_v1(uuid,jsonb,uuid,text,text,jsonb) FROM service_role;
