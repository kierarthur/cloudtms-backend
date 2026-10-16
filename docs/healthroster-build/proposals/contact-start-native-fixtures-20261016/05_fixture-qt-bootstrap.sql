-- Synthetic rollback-only deployment authority. This is NOT a deployment seed.
-- The root must have mounted the real empty schema; do not replace an installed
-- authority or delete/relabel its row to fit this fixture.
DO $bootstrap$
DECLARE v_policy jsonb:=pg_temp.contact_policy_v1('ALLOWED');
BEGIN
 PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM public.candidate_push_timing_policy)
  AND NOT EXISTS(SELECT 1 FROM public.candidate_push_timing_commands),'fixture QT authority must start vacant');
 INSERT INTO public.candidate_push_timing_policy(environment,agency_id,revision,policy_json,policy_sha256,updated_at_utc)
 VALUES('TEST','f7160000-0000-4000-8000-000000000019',1,v_policy,
  private.weekly_source_sha256_jsonb_v1('PUSH_TIMING_POLICY_V1',v_policy),clock_timestamp());
END;
$bootstrap$;
