-- Banking Pay bounded-scope V1.2.4: build-bound bounded input fingerprints.

CREATE OR REPLACE FUNCTION private.pay_workbench_timesheet_input_fingerprint_v1(
  p_build_id uuid,
  p_candidate_id uuid,
  p_timesheet_ids uuid[]
)
RETURNS TABLE(timesheet_id uuid,input_fingerprint text,revision_json jsonb)
LANGUAGE sql
STABLE
PARALLEL UNSAFE
SECURITY INVOKER
SET search_path = ''
AS $function$
WITH requested AS (
  SELECT DISTINCT requested_id AS timesheet_id
  FROM unnest(COALESCE(p_timesheet_ids,ARRAY[]::uuid[])) AS requested_id
  WHERE requested_id IS NOT NULL
), revisions AS (
  SELECT
    requested.timesheet_id,
    jsonb_strip_nulls(jsonb_build_object(
      'fingerprint_version',1,
      'build_id',p_build_id,
      'candidate_id',p_candidate_id,
      'timesheet_id',requested.timesheet_id,
      'timesheet_digest',md5(to_jsonb(timesheet_row)::text),
      'tsfin_digest',tsfin_revision.row_digest,
      'candidate_route_digest',md5(jsonb_build_object(
        'pay_method',to_jsonb(candidate_row)->'pay_method',
        'umbrella_id',to_jsonb(candidate_row)->'umbrella_id',
        'paye_state',to_jsonb(candidate_row)->'paye_state'
      )::text),
      'pay_state_digest',pay_state_revision.row_digest,
      'dirty_generation',scope_state.dirty_generation,
      'economic_state',scope_state.economic_state,
      'dependency_unit_key','UNIT:'||lower(build_scope.dependency_unit_anchor_timesheet_id::text),
      'dependency_unit_digest',anchor_scope.dependency_unit_digest,
      'override_revision',override_revision.revision_digest,
      'adjustment_revision',adjustment_revision.revision_digest,
      'snooze_revision',snooze_revision.revision_digest
    )) AS revision_json
  FROM requested
  JOIN public.timesheets AS timesheet_row
    ON timesheet_row.timesheet_id=requested.timesheet_id
  JOIN public.candidates AS candidate_row ON candidate_row.id=p_candidate_id
  JOIN private.banking_pay_workbench_timesheet_scope_state AS scope_state
    ON scope_state.timesheet_id=requested.timesheet_id
   AND scope_state.candidate_id=p_candidate_id
  JOIN private.banking_pay_workbench_economic_build_scope AS build_scope
    ON build_scope.build_id=p_build_id
   AND build_scope.timesheet_id=requested.timesheet_id
  JOIN private.banking_pay_workbench_economic_build_scope AS anchor_scope
    ON anchor_scope.build_id=build_scope.build_id
   AND anchor_scope.timesheet_id=build_scope.dependency_unit_anchor_timesheet_id
   AND anchor_scope.dependency_unit_digest IS NOT NULL
  LEFT JOIN LATERAL (
    SELECT md5(string_agg(md5(to_jsonb(financial_row)::text),'' ORDER BY financial_row.id)) AS row_digest
    FROM public.timesheets_financials AS financial_row
    WHERE financial_row.timesheet_id=requested.timesheet_id AND financial_row.is_current
      AND financial_row.candidate_id=p_candidate_id
  ) AS tsfin_revision ON true
  LEFT JOIN LATERAL (
    SELECT md5(to_jsonb(pay_state_row)::text) AS row_digest
    FROM public.timesheet_pay_state AS pay_state_row
    WHERE pay_state_row.timesheet_id=requested.timesheet_id
  ) AS pay_state_revision ON true
  LEFT JOIN LATERAL (
    SELECT md5(COALESCE(string_agg(
      override_row.id::text||':'||COALESCE(override_row.created_at_utc::text,'')||':'
      ||COALESCE(override_row.cleared_at_utc::text,'')||':'||COALESCE(override_row.consumed_at_utc::text,''),
      '' ORDER BY override_row.id),'')) AS revision_digest
    FROM public.timesheet_payment_overrides AS override_row
    WHERE override_row.candidate_id=p_candidate_id
      AND override_row.timesheet_id=requested.timesheet_id
      AND override_row.cleared_at_utc IS NULL
      AND override_row.consumed_at_utc IS NULL
      AND override_row.consumed_by_pay_batch_id IS NULL
  ) AS override_revision ON true
  LEFT JOIN LATERAL (
    SELECT md5(COALESCE(string_agg(
      adjustment_row.id::text||':'||COALESCE(adjustment_row.updated_at::text,''),
      '' ORDER BY adjustment_row.id),'')) AS revision_digest
    FROM public.ts_pay_adjustments AS adjustment_row
    WHERE adjustment_row.candidate_id=p_candidate_id
      AND adjustment_row.timesheet_id=requested.timesheet_id
      AND adjustment_row.paid_at_utc IS NULL
  ) AS adjustment_revision ON true
  LEFT JOIN LATERAL (
    SELECT md5(COALESCE(string_agg(
      snooze_row.id::text||':'||COALESCE(snooze_row.updated_at_utc::text,''),
      '' ORDER BY snooze_row.id),'')) AS revision_digest
    FROM public.pay_item_snoozes AS snooze_row
    WHERE snooze_row.candidate_id=p_candidate_id
      AND snooze_row.timesheet_id=requested.timesheet_id
      AND snooze_row.cleared_at_utc IS NULL AND snooze_row.cancelled_at_utc IS NULL
  ) AS snooze_revision ON true
)
SELECT revisions.timesheet_id,md5(revisions.revision_json::text),revisions.revision_json
FROM revisions
ORDER BY revisions.timesheet_id;
$function$;

ALTER FUNCTION private.pay_workbench_timesheet_input_fingerprint_v1(uuid,uuid,uuid[]) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_timesheet_input_fingerprint_v1(uuid,uuid,uuid[]) FROM PUBLIC,anon,authenticated,service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_timesheet_input_fingerprint_v1(uuid,uuid,uuid[]) TO postgres;
