-- Banking Pay bounded-scope V1.2.5: one immutable projection per sealed
-- dependency unit. The projection is independent of fact-page boundaries.

CREATE OR REPLACE FUNCTION private.pay_workbench_unit_projection_v1(
  p_build_id uuid,
  p_dependency_unit_key text DEFAULT NULL::text
)
RETURNS TABLE(
  dependency_unit_key text,
  scope_family_key text,
  family_timesheet_id uuid,
  canonical_timesheet_id uuid,
  projected_timesheet_id uuid
)
LANGUAGE sql
STABLE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path = ''
AS $function$
WITH members AS (
  SELECT
    scope_row.dependency_unit_key,
    COALESCE(NULLIF(BTRIM(timesheet_row.booking_id),''),scope_row.timesheet_id::text) AS scope_family_key,
    scope_row.timesheet_id AS family_timesheet_id,
    timesheet_row.is_current,
    timesheet_row.version,
    timesheet_row.updated_at,
    timesheet_row.created_at
  FROM private.banking_pay_workbench_economic_build_scope AS scope_row
  JOIN public.timesheets AS timesheet_row
    ON timesheet_row.timesheet_id=scope_row.timesheet_id
  WHERE scope_row.build_id=p_build_id
    AND scope_row.closure_status='SEALED'
    AND (p_dependency_unit_key IS NULL
      OR scope_row.dependency_unit_key=p_dependency_unit_key)
), canonical AS (
  SELECT DISTINCT ON (member.dependency_unit_key,member.scope_family_key)
    member.dependency_unit_key,
    member.scope_family_key,
    member.family_timesheet_id AS canonical_timesheet_id
  FROM members AS member
  WHERE member.is_current IS TRUE
  ORDER BY member.dependency_unit_key,member.scope_family_key,
    member.version DESC NULLS LAST,member.updated_at DESC NULLS LAST,
    member.created_at DESC NULLS LAST,member.family_timesheet_id
)
SELECT
  member.dependency_unit_key,
  member.scope_family_key,
  member.family_timesheet_id,
  canonical.canonical_timesheet_id,
  canonical.canonical_timesheet_id AS projected_timesheet_id
FROM members AS member
LEFT JOIN canonical
  ON canonical.dependency_unit_key=member.dependency_unit_key
 AND canonical.scope_family_key=member.scope_family_key
ORDER BY member.dependency_unit_key,member.scope_family_key,
  member.family_timesheet_id;
$function$;

ALTER FUNCTION private.pay_workbench_unit_projection_v1(uuid,text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_unit_projection_v1(uuid,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION private.pay_workbench_unit_projection_v1(uuid,text) FROM anon;
REVOKE ALL ON FUNCTION private.pay_workbench_unit_projection_v1(uuid,text) FROM authenticated;
REVOKE ALL ON FUNCTION private.pay_workbench_unit_projection_v1(uuid,text) FROM service_role;
GRANT EXECUTE ON FUNCTION private.pay_workbench_unit_projection_v1(uuid,text) TO postgres;

COMMENT ON FUNCTION private.pay_workbench_unit_projection_v1(uuid,text) IS
  'Returns the complete sealed dependency-unit family projection. It never expands from an arbitrary fact-page subset.';
