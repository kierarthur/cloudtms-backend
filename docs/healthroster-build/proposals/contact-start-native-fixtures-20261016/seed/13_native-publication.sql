-- Native complete upload/seal/projection/current publication. No manual CURRENT,
-- comparison, incident, WorkEvent, generation, intent or audit INSERT is allowed here.
DO $publish$
DECLARE a jsonb;v_cycle uuid;v_actor uuid:='f716c000-0000-4000-8000-000000000003';
 v_begin jsonb;v_stage jsonb;v_seal jsonb;v_build jsonb;v_context jsonb;v_apply jsonb;v_publish jsonb;
 v_row jsonb;v_expected jsonb;v_contract jsonb;v_ec jsonb;v_template jsonb;v_rows jsonb:='[]'::jsonb;
 v_publication uuid;v_upload uuid;v_audit jsonb;v_candidate_ids uuid[];
BEGIN
 SELECT value INTO STRICT a FROM pg_temp.contact_native_seed_artifact;
 SELECT (value->>'source_cycle_id')::uuid INTO STRICT v_cycle FROM pg_temp.contact_seed_receipts WHERE name='native_cycle';
 v_begin:=public.weekly_source_upload_stage_begin_atomic_v1(a->'begin_request'||jsonb_build_object('source_cycle_id',v_cycle));
 PERFORM pg_temp.contact_assert_v1(v_begin->>'status'='STAGING','real upload BEGIN');
 v_upload:=(v_begin->>'logical_upload_id')::uuid;
 v_stage:=public.weekly_source_upload_stage_rows_atomic_v1(a->'stage_request'||jsonb_build_object('actor_user_id',v_actor,'upload_id',v_upload));
 v_seal:=public.weekly_source_upload_seal_atomic_v1(jsonb_build_object('actor_user_id',v_actor,'upload_id',v_upload));
 PERFORM pg_temp.contact_assert_v1(v_seal->>'status'='CURRENT' AND v_seal->>'row_manifest_hash'~'^[0-9a-f]{64}$','real immutable complete seal');
 v_build:=public.weekly_source_projection_begin_atomic_v1(jsonb_build_object('actor_user_id',v_actor,'upload_id',v_upload,
  'expected_authority_scope_version',v_seal->'authority_scope_version'));
 PERFORM pg_temp.contact_assert_v1(v_build->>'status'='BUILDING','native BUILDING allocation');
 v_publication:=(v_build->>'publication_id')::uuid;
 v_context:=public.weekly_source_upload_context_v1(jsonb_build_object('operation','BUILD_PROJECTION','actor_user_id',v_actor,
  'source_group_id','f716c000-0000-4000-8000-000000000001','source_cycle_id',v_cycle,
  'client_id','f716c000-0000-4000-8000-000000000020','upload_id',v_upload));
 PERFORM pg_temp.contact_assert_v1(v_context->>'profile_id'=a->'report'->>'profile_id'
  AND v_context->>'authority_scope_kind'='CYCLE' AND jsonb_array_length(v_context->'rows')=2,'real native zero context census');
 FOR v_row IN SELECT value FROM jsonb_array_elements(v_context->'rows') LOOP
  SELECT value INTO STRICT v_expected FROM jsonb_array_elements(a->'expected_context'->'rows')
   WHERE value->'source_row_ordinal'=v_row->'source_row_ordinal';
  -- Every fact the existing zero-row projection producer reads is compared
  -- with the actual native context. Hash and allocated row ID are bound only now.
  PERFORM pg_temp.contact_assert_v1(v_row->'candidate_match_count'=v_expected->'candidate_match_count'
   AND v_row->'candidate_id'=v_expected->'candidate_id' AND v_row->'client_id'=v_expected->'client_id'
   AND v_row->'external_source_key'=v_expected->'external_source_key' AND v_row->'work_date'=v_expected->'work_date'
   AND v_row->>'row_finalisation_state'='SOURCE_ABSENT_ZERO' AND (v_row->>'actual_net_minutes')::integer=0
   AND v_row->'start_at_local'=v_expected->'start_at_local' AND v_row->'end_at_local'=v_expected->'end_at_local'
   AND v_row->'break_minutes'=v_expected->'break_minutes'
   AND v_row->>'prior_work_event_id' IS NULL AND v_row->>'prior_accepted_contract_id' IS NULL
   AND v_row->>'office_selected_contract_id' IS NULL AND v_row->>'office_selected_work_event_id' IS NULL
   AND NOT coalesce((v_row->>'office_separate_shift')::boolean,false)
   AND NOT coalesce((v_row->>'prior_work_event_ambiguous')::boolean,false)
   AND v_row->'protected_matches'='[]'::jsonb AND jsonb_array_length(v_row->'contracts')=1,
   'actual native context differs from the zero-only expected fixture inputs');
  v_contract:=v_row->'contracts'->0;v_ec:=v_expected->'contracts'->0;
  PERFORM pg_temp.contact_assert_v1(v_contract->'contract_id'=v_ec->'contract_id'
   AND v_contract->'candidate_id'=v_ec->'candidate_id' AND v_contract->'client_id'=v_ec->'client_id'
   AND v_contract->'valid_from'=v_ec->'valid_from' AND v_contract->'valid_to'=v_ec->'valid_to'
   AND v_contract->'weekly_source_applicable'='true'::jsonb AND v_contract->'schedule_compatible'='true'::jsonb
   AND coalesce((v_contract->>'verified_role_band_match')::boolean,false)=false
   AND v_contract->'effective_policy'->>'authority_mode'='SOURCE_AUTHORITY'
   AND v_contract->'effective_policy'->>'c1_source_mode'='HEALTHROSTER_WEEKLY'
   AND v_contract->'effective_policy'->>'policy_sha256'~'^[0-9a-f]{64}$',
   'actual contract applicability/policy differs from existing projection producer inputs');
  SELECT value INTO STRICT v_template FROM jsonb_array_elements(a->'projection_templates')
   WHERE value->'source_row_ordinal'=v_row->'source_row_ordinal';
  v_rows:=v_rows||jsonb_build_array((v_template-'source_row_ordinal')||jsonb_build_object('upload_row_id',v_row->'upload_row_id',
   'effective_policy_fingerprint',v_contract->'effective_policy'->'policy_sha256'));
 END LOOP;
 v_apply:=public.weekly_source_projection_rows_apply_atomic_v1(v_actor,v_publication,v_rows);
 PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM public.weekly_source_row_resolutions r
  JOIN public.weekly_source_upload_rows u ON u.id=r.upload_row_id WHERE u.upload_id=v_upload
   AND(r.mapping_state<>'RESOLVED' OR r.paid_minutes<>0 OR r.rate_classifications_json IS NOT NULL)),'native mapping must remain resolved/nonfinancial');
 v_publish:=public.weekly_source_projection_publish_atomic_v1(jsonb_build_object('actor_user_id',v_actor,'publication_id',v_publication));
 PERFORM pg_temp.contact_assert_v1(v_publish->>'status'='CURRENT' AND v_publish->>'publication_fingerprint'~'^[0-9a-f]{64}$','real native CURRENT publication');
 PERFORM private.weekly_source_query_current_publication_v1(v_cycle,v_publication);
 SELECT jsonb_agg(jsonb_build_object('timesheet_id',after_json->>'timesheet_id','state',after_json->>'state',
  'refusal_code',after_json->>'refusal_code') ORDER BY after_json->>'timesheet_id') INTO v_audit
 FROM public.audit_events WHERE object_type='timesheets' AND action='WEEKLY_SOURCE_PUBLICATION_COMPARISON'
  AND after_json->>'projection_publication_id'=v_publication::text AND event_sequence_is_authoritative;
 PERFORM pg_temp.contact_assert_v1(jsonb_array_length(v_audit)=3 AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(v_audit) x
  WHERE x->>'state'<>'COMPARED' OR x->>'refusal_code' IS NOT NULL), 'all three genuine signed declarations must be COMPARED: '||coalesce(v_audit::text,'NO_NATIVE_AUDIT'));
 PERFORM pg_temp.contact_assert_v1((SELECT count(*)=3 FROM public.weekly_discrepancy_incidents i
  JOIN public.weekly_issue_comparison_revisions c ON c.id=i.current_comparison_revision_id
  WHERE i.source_cycle_id=v_cycle AND i.state='OPEN' AND c.projection_publication_id=v_publication
   AND c.candidate_timesheet_id IS NOT NULL AND(
    (i.candidate_id='f716c000-0000-4000-8000-000000000010' AND c.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED')
    OR(i.candidate_id IN('f716c000-0000-4000-8000-000000000011','f716c000-0000-4000-8000-000000000012')
     AND c.issue_family='SOURCE_MISSING_OR_NOT_AUTHORISED'))),
  'genuine missing-A/zero-B/C comparison topology required');
 INSERT INTO pg_temp.contact_seed_receipts VALUES('native_upload_begin',v_begin),('native_upload_stage',v_stage),
  ('native_upload_seal',v_seal),('native_projection_begin',v_build),('native_projection_apply',v_apply),
  ('native_projection_publish',v_publish),('native_comparison_audit',v_audit);
END;
$publish$;
