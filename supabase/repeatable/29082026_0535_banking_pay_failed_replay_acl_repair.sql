-- Restore the two service-only RPC ACLs that an interrupted historical
-- compatibility replay could temporarily reopen.  Function bodies, payment
-- economics, Draft ownership and provider behaviour are unchanged.

ALTER FUNCTION public.bulk_timesheet_row_patch_v1(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.bulk_timesheet_row_patch_v1(jsonb)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.bulk_timesheet_row_patch_v1(jsonb)
  TO service_role;

ALTER FUNCTION public.timesheet_daily_manual_process_atomic(
  uuid,
  uuid,
  uuid,
  jsonb,
  jsonb,
  timestamptz,
  text
) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.timesheet_daily_manual_process_atomic(
  uuid,
  uuid,
  uuid,
  jsonb,
  jsonb,
  timestamptz,
  text
) FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.timesheet_daily_manual_process_atomic(
  uuid,
  uuid,
  uuid,
  jsonb,
  jsonb,
  timestamptz,
  text
) TO service_role;
