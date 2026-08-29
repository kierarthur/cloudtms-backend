-- Retain the installed compatibility body unchanged, but remove it from every
-- application execution role. Workbench refresh is owned by the bounded
-- PAYMENT_CORRECTION REFRESH_WORKBENCH phase.
ALTER FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_with_workbench_refr(uuid,jsonb,uuid,text,text,jsonb,uuid,bigint,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_with_workbench_refr(uuid,jsonb,uuid,text,text,jsonb,uuid,bigint,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_with_workbench_refr(uuid,jsonb,uuid,text,text,jsonb,uuid,bigint,text) FROM anon;
REVOKE ALL ON FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_with_workbench_refr(uuid,jsonb,uuid,text,text,jsonb,uuid,bigint,text) FROM authenticated;
REVOKE ALL ON FUNCTION public.pay_payment_cancel_not_sent_and_recalculate_with_workbench_refr(uuid,jsonb,uuid,text,text,jsonb,uuid,bigint,text) FROM service_role;
