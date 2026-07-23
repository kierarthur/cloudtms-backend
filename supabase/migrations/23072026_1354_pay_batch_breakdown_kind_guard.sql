CREATE OR REPLACE FUNCTION public._pay_batch_item_breakdown_kind_guard_v1()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path TO 'public'
AS $function$
DECLARE
  v_item_type text;
BEGIN
  SELECT upper(btrim(coalesce(batch_item.item_type, '')))
  INTO v_item_type
  FROM public.pay_batch_items AS batch_item
  WHERE batch_item.id = NEW.pay_batch_item_id;

  IF v_item_type IN (
    'OVERPAYMENT_RECOVERY',
    'LOAN_REPAYMENT',
    'MANUAL_DEBT_RECOVERY',
    'MANUAL_CREDIT_PAYOUT',
    'LOAN_PAYOUT',
    'UNDERPAYMENT_PAYMENT',
    'DEBT_CREATED'
  ) THEN
    NEW.line_kind := v_item_type;
  END IF;

  RETURN NEW;
END;
$function$;

REVOKE ALL ON FUNCTION public._pay_batch_item_breakdown_kind_guard_v1() FROM PUBLIC, anon, authenticated;

DROP TRIGGER IF EXISTS pay_batch_item_breakdown_kind_guard_v1
ON public.pay_batch_item_breakdowns;

CREATE TRIGGER pay_batch_item_breakdown_kind_guard_v1
BEFORE INSERT OR UPDATE OF pay_batch_item_id, line_kind
ON public.pay_batch_item_breakdowns
FOR EACH ROW
EXECUTE FUNCTION public._pay_batch_item_breakdown_kind_guard_v1();
