/* Banking Pay targeted fast-route least-privilege boundary.
 * Reasserted after the final removal of the legacy monolith's prepare drop.
 */

ALTER FUNCTION public.pay_workbench_session_clone_eligible_rows_v1(uuid,uuid,integer,jsonb,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_session_clone_eligible_rows_v1(uuid,uuid,integer,jsonb,jsonb)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_session_clone_eligible_rows_v1(uuid,uuid,integer,jsonb,jsonb)
  TO service_role;

ALTER FUNCTION public.pay_workbench_candidate_delta_refresh_chunk(uuid,uuid,jsonb,jsonb,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_candidate_delta_refresh_chunk(uuid,uuid,jsonb,jsonb,integer)
  FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION public.pay_workbench_candidate_delta_refresh_chunk(uuid,uuid,jsonb,jsonb,integer)
  TO service_role;

ALTER FUNCTION public.pay_workbench_delta_write_compatible_rows_v1(uuid,uuid,uuid,jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.pay_workbench_delta_write_compatible_rows_v1(uuid,uuid,uuid,jsonb)
  FROM PUBLIC,anon,authenticated,service_role;
