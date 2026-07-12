
-- GENUINELY NEW IMPLEMENTATION UNIT
-- Exact owner/ACL binding for the three internal authority tables introduced by
-- this package.  This deliberately does not enumerate functions by name or
-- rewrite inherited RPC ACLs: amended functions retain their installed ACLs,
-- and every genuinely new public/internal function sets its own exact ACL in
-- its defining file.
DO $do$
DECLARE
  v_marker_writer regprocedure := to_regprocedure(
    'public.timesheet_financial_retention_mark_v1(uuid[])'
  );
  v_archive_guard regprocedure := to_regprocedure(
    'public.timesheet_archive_row_guard_v1()'
  );
  v_archive_transition regprocedure := to_regprocedure(
    'public.timesheet_archive_transition_v1(uuid,text,text,uuid,uuid,text,timestamptz)'
  );
  v_r2_claim regprocedure := to_regprocedure(
    'public.timesheet_r2_cleanup_claim_v1(integer,integer)'
  );
  v_r2_record regprocedure := to_regprocedure(
    'public.timesheet_r2_cleanup_record_v1(text,uuid,uuid[],jsonb,uuid)'
  );
  v_r2_complete regprocedure := to_regprocedure(
    'public.timesheet_r2_cleanup_complete_v1(text,uuid,text[])'
  );
  v_marker_owner oid;
  v_archive_guard_owner oid;
  v_archive_transition_owner oid;
  v_r2_claim_owner oid;
  v_r2_record_owner oid;
  v_r2_complete_owner oid;
  v_owner_name name;
  v_role name;
BEGIN
  IF v_marker_writer IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'RETENTION_MARKER_WRITER_NOT_INSTALLED';
  END IF;
  IF v_archive_guard IS NULL OR v_archive_transition IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'ARCHIVE_AUTHORITY_FUNCTIONS_NOT_INSTALLED';
  END IF;
  IF v_r2_claim IS NULL OR v_r2_record IS NULL OR v_r2_complete IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'R2_CLEANUP_FUNCTIONS_NOT_INSTALLED';
  END IF;

  SELECT p.proowner INTO v_marker_owner
  FROM pg_catalog.pg_proc AS p
  WHERE p.oid = v_marker_writer::oid;

  SELECT p.proowner INTO v_archive_guard_owner
  FROM pg_catalog.pg_proc AS p
  WHERE p.oid = v_archive_guard::oid;

  SELECT p.proowner INTO v_archive_transition_owner
  FROM pg_catalog.pg_proc AS p
  WHERE p.oid = v_archive_transition::oid;

  IF v_archive_guard_owner IS DISTINCT FROM v_archive_transition_owner THEN
    RAISE EXCEPTION USING
      MESSAGE = 'ARCHIVE_AUTHORITY_OWNER_MISMATCH',
      DETAIL = jsonb_build_object(
        'guard_owner', pg_catalog.pg_get_userbyid(v_archive_guard_owner),
        'transition_owner', pg_catalog.pg_get_userbyid(v_archive_transition_owner)
      )::text;
  END IF;

  SELECT p.proowner INTO v_r2_claim_owner
  FROM pg_catalog.pg_proc AS p
  WHERE p.oid = v_r2_claim::oid;

  SELECT p.proowner INTO v_r2_record_owner
  FROM pg_catalog.pg_proc AS p
  WHERE p.oid = v_r2_record::oid;

  SELECT p.proowner INTO v_r2_complete_owner
  FROM pg_catalog.pg_proc AS p
  WHERE p.oid = v_r2_complete::oid;

  IF v_r2_claim_owner IS DISTINCT FROM v_r2_record_owner
     OR v_r2_claim_owner IS DISTINCT FROM v_r2_complete_owner THEN
    RAISE EXCEPTION USING
      MESSAGE = 'R2_CLEANUP_OWNER_MISMATCH',
      DETAIL = jsonb_build_object(
        'claim_owner', pg_catalog.pg_get_userbyid(v_r2_claim_owner),
        'record_owner', pg_catalog.pg_get_userbyid(v_r2_record_owner),
        'complete_owner', pg_catalog.pg_get_userbyid(v_r2_complete_owner)
      )::text;
  END IF;

  SELECT r.rolname INTO v_owner_name
  FROM pg_catalog.pg_roles AS r
  WHERE r.oid = v_marker_owner;
  IF v_owner_name IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'RETENTION_MARKER_OWNER_NOT_FOUND';
  END IF;
  EXECUTE pg_catalog.format(
    'ALTER TABLE public.timesheet_financial_retention OWNER TO %I',
    v_owner_name
  );

  SELECT r.rolname INTO v_owner_name
  FROM pg_catalog.pg_roles AS r
  WHERE r.oid = v_archive_guard_owner;
  IF v_owner_name IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'ARCHIVE_AUTHORITY_OWNER_NOT_FOUND';
  END IF;
  EXECUTE pg_catalog.format(
    'ALTER TABLE public.timesheet_archive_transition_capability OWNER TO %I',
    v_owner_name
  );

  SELECT r.rolname INTO v_owner_name
  FROM pg_catalog.pg_roles AS r
  WHERE r.oid = v_r2_claim_owner;
  IF v_owner_name IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'R2_CLEANUP_OWNER_NOT_FOUND';
  END IF;
  EXECUTE pg_catalog.format(
    'ALTER TABLE public.timesheet_r2_cleanup_queue OWNER TO %I',
    v_owner_name
  );

  REVOKE ALL ON TABLE public.timesheet_financial_retention FROM PUBLIC;
  REVOKE ALL ON TABLE public.timesheet_archive_transition_capability FROM PUBLIC;
  REVOKE ALL ON TABLE public.timesheet_r2_cleanup_queue FROM PUBLIC;

  FOREACH v_role IN ARRAY ARRAY['anon', 'authenticated', 'service_role']::name[] LOOP
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles AS r WHERE r.rolname = v_role) THEN
      EXECUTE pg_catalog.format(
        'REVOKE ALL ON TABLE public.timesheet_financial_retention FROM %I',
        v_role
      );
      EXECUTE pg_catalog.format(
        'REVOKE ALL ON TABLE public.timesheet_archive_transition_capability FROM %I',
        v_role
      );
      EXECUTE pg_catalog.format(
        'REVOKE ALL ON TABLE public.timesheet_r2_cleanup_queue FROM %I',
        v_role
      );
    END IF;
  END LOOP;
END
$do$;
