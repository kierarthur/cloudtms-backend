-- A retry of the September H1/H2 final-authority closure can replay its older
-- one-candidate cancellation and no-money workbench helpers after their later
-- reviewed successors were already recorded in the ledger. Reassert those
-- exact successors first; the other current helpers remain unchanged.
-- The historical plpgsql_check workaround is not a Banking Pay rule. It must
-- be reasserted after every reviewed finalizer authority closure: an UPGRADE
-- can replay a later closure after 20072026_0302 and lose this function-local
-- setting even though the finalizer body remains byte-identical.
--
-- Refuse an unreviewed body/configuration before changing the setting. The
-- two pins differ only by PostgreSQL's displayed quoted instrumentation line;
-- the installed TEST body and the repository V8 body were compared exactly.
\ir 07092026_2013_banking_pay_unpaid_cancellation_communication_v2_prepare_v1.sql
\ir 09092026_0020_banking_pay_no_money_workbench_return_v1.sql

DO $finalizer_instrumentation_guard$
DECLARE
  v_definition text;
  v_hash text;
BEGIN
  SELECT replace(pg_catalog.pg_get_functiondef(p.oid), chr(13) || chr(10), chr(10))
    INTO v_definition
    FROM pg_catalog.pg_proc AS p
   WHERE p.oid = 'public.pay_batch_finalize_reservations_and_markers(uuid,text,uuid,date,date,uuid,jsonb)'::regprocedure;

  v_hash := pg_catalog.encode(
    pg_catalog.sha256(pg_catalog.convert_to(v_definition, 'UTF8')), 'hex');
  IF v_hash NOT IN (
    '0449fe0daa2d6e1b23aa7d991bfce84d84fce30d7d123dff6804cbe528ddc4a4',
    '49fbfaa7e3fbcefe7fde5e6937f2ac2b772fc62528460ae22246f98f7ad01d08'
  ) THEN
    RAISE EXCEPTION 'FINALIZER_INSTRUMENTATION_UNREVIEWED_DEFINITION: %', v_hash;
  END IF;
END
$finalizer_instrumentation_guard$;

ALTER FUNCTION public.pay_batch_finalize_reservations_and_markers(
  uuid, text, uuid, date, date, uuid, jsonb
) SET plpgsql_check.mode TO 'disabled';
