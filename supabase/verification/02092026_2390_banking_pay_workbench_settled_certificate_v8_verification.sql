\set ON_ERROR_STOP on

-- Provider-neutral catalogue/security verification for the H1 Workbench
-- settled-certificate producer and bounded reader surface. This verifier is
-- read-only: it creates no certificate, Draft, payment or provider state.
DO $verification$
DECLARE
  v_relation text;
  v_identity text;
  v_oid oid;
  v_owner text;
  v_security_definer boolean;
  v_config text[];
  v_missing_indexes text[];
BEGIN
  FOREACH v_relation IN ARRAY ARRAY[
    'private.banking_pay_workbench_settled_certificates_v8',
    'private.banking_pay_workbench_settled_certificate_source_members_v8',
    'private.banking_pay_workbench_settled_certificate_publications_v8',
    'private.banking_pay_workbench_settled_certificate_universes_v8',
    'private.banking_pay_workbench_settled_certificate_universe_members_v8',
    'private.banking_pay_workbench_settled_certificate_entries_v8',
    'private.banking_pay_workbench_settled_certificate_superseded_sources_v8',
    'private.banking_pay_workbench_settled_cert_source_reservations_v8',
    'private.banking_pay_workbench_settled_certificate_component_evidence_v8',
    'private.banking_pay_workbench_settled_cert_filter_scope_manifest_v8',
    'private.banking_pay_workbench_settled_certificate_partitions_v8',
    'private.banking_pay_workbench_settled_certificate_partition_members_v8',
    'private.banking_pay_workbench_settled_certificate_channel_manifests_v8',
    'private.banking_pay_workbench_settled_certificate_policy_owners_v8',
    'private.banking_pay_workbench_settled_certificate_policy_surfaces_v8',
    'private.banking_pay_workbench_settled_certificate_digest_runs_v8',
    'private.banking_pay_workbench_settled_certificate_digest_checkpoints_v8',
    'private.banking_pay_workbench_settled_certificate_page_receipts_v8',
    'private.banking_pay_workbench_settled_certificate_lifecycle_events_v8',
    'private.banking_pay_workbench_settled_certificate_operation_links_v8',
    'private.banking_pay_workbench_settled_certificate_audit_attestations_v8'
  ] LOOP
    IF pg_catalog.to_regclass(v_relation) IS NULL THEN
      RAISE EXCEPTION 'WORKBENCH_SETTLED_CERTIFICATE_V8_RELATION_MISSING: %', v_relation;
    END IF;
    IF (
      SELECT pg_catalog.pg_get_userbyid(class_row.relowner)
      FROM pg_catalog.pg_class class_row
      WHERE class_row.oid=pg_catalog.to_regclass(v_relation)
    ) NOT IN (CURRENT_USER,'postgres') THEN
      RAISE EXCEPTION 'WORKBENCH_SETTLED_CERTIFICATE_V8_RELATION_OWNER_INVALID: %', v_relation;
    END IF;
    IF pg_catalog.has_table_privilege('anon',v_relation,'SELECT,INSERT,UPDATE,DELETE')
       OR pg_catalog.has_table_privilege('authenticated',v_relation,'SELECT,INSERT,UPDATE,DELETE')
       OR pg_catalog.has_table_privilege('service_role',v_relation,'SELECT,INSERT,UPDATE,DELETE') THEN
      RAISE EXCEPTION 'WORKBENCH_SETTLED_CERTIFICATE_V8_RELATION_ACL_INVALID: %', v_relation;
    END IF;
  END LOOP;

  -- These are the only service-callable H1 V8 functions. They keep the
  -- existing 256-row page limit and the existing operation-start budgets.
  FOREACH v_identity IN ARRAY ARRAY[
    'public.pay_workbench_settled_certificate_build_start_v8(uuid,uuid,text)',
    'public.pay_workbench_settled_certificate_build_append_page_v8(uuid,integer,integer,text,text)',
    'public.pay_workbench_settled_certificate_seal_v8(uuid,uuid)',
    'public.pay_workbench_settled_certificate_lifecycle_v8(text,text,text,uuid)',
    'public.pay_workbench_settled_certificate_current_reference_issue_v8(uuid,bigint,bigint,text,text,jsonb)',
    'public.pay_workbench_settled_certificate_entry_page_v8(uuid,text,integer,integer,text)',
    'public.pay_workbench_settled_certificate_partition_page_v8(uuid,text,integer,integer,text)',
    'public.pay_workbench_settled_certificate_component_page_v8(uuid,text,text,integer,integer,text)',
    'public.pay_workbench_settled_certificate_filter_manifest_v8(uuid,text)',
    'public.banking_pay_draft_certified_operation_start_v8(jsonb,uuid,text)'
  ] LOOP
    v_oid:=pg_catalog.to_regprocedure(v_identity);
    IF v_oid IS NULL THEN
      RAISE EXCEPTION 'WORKBENCH_SETTLED_CERTIFICATE_V8_FUNCTION_MISSING: %', v_identity;
    END IF;
    SELECT pg_catalog.pg_get_userbyid(proc.proowner),proc.prosecdef,proc.proconfig
    INTO STRICT v_owner,v_security_definer,v_config
    FROM pg_catalog.pg_proc proc WHERE proc.oid=v_oid;
    IF v_owner NOT IN (CURRENT_USER,'postgres')
       OR NOT v_security_definer
       OR NOT ('search_path=""'=ANY(coalesce(v_config,ARRAY[]::text[])))
       OR pg_catalog.has_function_privilege('anon',v_oid,'EXECUTE')
       OR pg_catalog.has_function_privilege('authenticated',v_oid,'EXECUTE')
       OR NOT pg_catalog.has_function_privilege('service_role',v_oid,'EXECUTE') THEN
      RAISE EXCEPTION 'WORKBENCH_SETTLED_CERTIFICATE_V8_FUNCTION_SECURITY_INVALID: %', v_identity;
    END IF;
  END LOOP;

  -- The unbound reference issuer and every private helper remain callable by
  -- the database owner only. The public service route can issue only the
  -- current-session-bound reference above.
  v_identity:='public.pay_workbench_settled_certificate_reference_issue_v8(text,text,text,jsonb)';
  v_oid:=pg_catalog.to_regprocedure(v_identity);
  IF v_oid IS NULL
     OR pg_catalog.has_function_privilege('anon',v_oid,'EXECUTE')
     OR pg_catalog.has_function_privilege('authenticated',v_oid,'EXECUTE')
     OR pg_catalog.has_function_privilege('service_role',v_oid,'EXECUTE')
     OR NOT pg_catalog.has_function_privilege(CURRENT_USER,v_oid,'EXECUTE') THEN
    RAISE EXCEPTION 'WORKBENCH_SETTLED_CERTIFICATE_V8_OWNER_ONLY_ISSUER_INVALID';
  END IF;

  FOREACH v_identity IN ARRAY ARRAY[
    'private.pay_workbench_settled_certificate_source_row_digest_v8(uuid)',
    'private.pay_workbench_settled_certificate_preview_contract_v8(uuid)',
    'private.pay_workbench_settled_certificate_constituent_seed_v8(uuid,integer)',
    'private.pay_workbench_settled_certificate_component_seed_v8(jsonb,integer,text,text)',
    'private.pay_workbench_settled_certificate_stable_stringify_v8(jsonb)',
    'private.pay_workbench_settled_certificate_sha256_init_v8()',
    'private.pay_workbench_settled_certificate_sha256_update_bytes_v8(jsonb,bytea,boolean)',
    'private.pay_workbench_settled_certificate_sha256_update_v8(jsonb,text)',
    'private.pay_workbench_settled_certificate_sha256_final_v8(jsonb)',
    'private.pay_workbench_settled_certificate_sha256_text_v8(text)',
    'private.pay_workbench_settled_certificate_money_v8(jsonb)',
    'private.pay_workbench_settled_certificate_component_evidence_json_v8(uuid,integer)',
    'private.pay_workbench_settled_certificate_constituent_unsigned_v8(uuid,integer)',
    'private.pay_workbench_settled_certificate_constituent_json_v8(uuid,integer)',
    'private.pay_workbench_settled_certificate_canonical_stream_page_v8(uuid,text,text,integer,integer)',
    'private.pay_workbench_settled_certificate_overall_digest_advance_v8(uuid,integer)',
    'private.workbench_settled_cert_same_week_override_validate_v8(jsonb)',
    'private.pay_workbench_settled_certificate_operation_admit_v8(uuid)',
    'private.pay_workbench_settled_certificate_digest_checkpoint_apply_v8(uuid,integer,jsonb,text)'
  ] LOOP
    v_oid:=pg_catalog.to_regprocedure(v_identity);
    IF v_oid IS NULL
       OR pg_catalog.has_function_privilege('anon',v_oid,'EXECUTE')
       OR pg_catalog.has_function_privilege('authenticated',v_oid,'EXECUTE')
       OR pg_catalog.has_function_privilege('service_role',v_oid,'EXECUTE')
       OR NOT pg_catalog.has_function_privilege(CURRENT_USER,v_oid,'EXECUTE') THEN
      RAISE EXCEPTION 'WORKBENCH_SETTLED_CERTIFICATE_V8_PRIVATE_FUNCTION_SECURITY_INVALID: %', v_identity;
    END IF;
  END LOOP;

  SELECT pg_catalog.array_agg(required.index_name ORDER BY required.index_name)
  INTO v_missing_indexes
  FROM pg_catalog.unnest(ARRAY[
    'banking_pay_wb_cert_v8_current_session_idx',
    'banking_pay_wb_cert_source_members_v8_page_idx',
    'banking_pay_wb_cert_entries_v8_page_idx',
    'banking_pay_wb_cert_entries_v8_channel_page_idx',
    'banking_pay_wb_cert_partitions_v8_page_idx',
    'banking_pay_wb_cert_partitions_v8_channel_page_idx',
    'banking_pay_wb_cert_members_v8_page_idx',
    'banking_pay_wb_cert_members_v8_stream_idx',
    'banking_pay_wb_cert_manifests_v8_scope_idx',
    'banking_pay_wb_cert_links_v8_cert_operation_idx'
  ]::text[]) AS required(index_name)
  WHERE pg_catalog.to_regclass('private.'||required.index_name) IS NULL;
  IF v_missing_indexes IS NOT NULL THEN
    RAISE EXCEPTION 'WORKBENCH_SETTLED_CERTIFICATE_V8_INDEXES_MISSING: %',v_missing_indexes;
  END IF;

  IF NOT EXISTS (
       SELECT 1 FROM pg_catalog.pg_attribute attr
       WHERE attr.attrelid='public.banking_pay_workbench_sessions'::regclass
         AND attr.attname='authority_fence_generation'
         AND NOT attr.attisdropped
         AND attr.attnotnull
     )
     OR NOT EXISTS (
       SELECT 1 FROM pg_catalog.pg_trigger trigger_row
       WHERE trigger_row.tgrelid='public.banking_pay_workbench_sessions'::regclass
         AND trigger_row.tgname='banking_pay_workbench_session_authority_fence_insert_v1'
         AND NOT trigger_row.tgisinternal
     )
     OR NOT EXISTS (
       SELECT 1 FROM pg_catalog.pg_trigger trigger_row
       WHERE trigger_row.tgrelid='public.banking_pay_workbench_sessions'::regclass
         AND trigger_row.tgname='banking_pay_workbench_session_authority_fence_update_v1'
         AND NOT trigger_row.tgisinternal
     ) THEN
    RAISE EXCEPTION 'WORKBENCH_SETTLED_CERTIFICATE_V8_SESSION_FENCE_INVALID';
  END IF;

  IF NOT EXISTS (
       SELECT 1 FROM pg_catalog.pg_proc proc
       WHERE proc.oid='public.banking_pay_operation_start(text,uuid,text,uuid,uuid,uuid,jsonb,jsonb)'::regprocedure
         AND proc.proconfig @> ARRAY['statement_timeout=6000ms','lock_timeout=1000ms']::text[]
     ) THEN
    RAISE EXCEPTION 'WORKBENCH_SETTLED_CERTIFICATE_V8_OPERATION_START_BUDGET_INVALID';
  END IF;
END
$verification$;
