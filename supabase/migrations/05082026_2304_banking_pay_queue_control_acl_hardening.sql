-- Banking Pay bounded-scope Version 1.2.16 queue-control ACL hardening.
-- TEST-only deployment artifact. No financial, Policy X, or public contract change.

ALTER FUNCTION public.pay_workbench_session_replay_replaced_queue_v1(uuid, uuid, text, jsonb)
  SET search_path TO '';
ALTER FUNCTION public.pay_workbench_session_replay_replaced_queue_v1(uuid, uuid, text, jsonb)
  OWNER TO postgres;

REVOKE ALL ON FUNCTION public.pay_workbench_session_replay_replaced_queue_v1(uuid, uuid, text, jsonb)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_replay_replaced_queue_v1(uuid, uuid, text, jsonb)
  TO postgres, service_role;

ALTER FUNCTION public.pay_workbench_fail_job(uuid, jsonb, integer)
  SET search_path TO '';
ALTER FUNCTION public.pay_workbench_fail_job(uuid, jsonb, integer)
  OWNER TO postgres;

REVOKE ALL ON FUNCTION public.pay_workbench_fail_job(uuid, jsonb, integer)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_fail_job(uuid, jsonb, integer)
  TO postgres, service_role;

NOTIFY pgrst, 'reload schema';
