-- Observation only. No native result/authority/guard is replaced.
-- Outer server rollback owns these AFTER INSERT observers and all fixture data.
CREATE TEMP TABLE contact_seed_observers(relation_name text PRIMARY KEY,trigger_name text NOT NULL) ON COMMIT DROP;
CREATE TEMP TABLE contact_seed_receipts(name text PRIMARY KEY,value jsonb NOT NULL) ON COMMIT DROP;
CREATE OR REPLACE FUNCTION pg_temp.contact_seed_capture_insert_v1()
RETURNS trigger LANGUAGE plpgsql AS $capture$
DECLARE key text;v jsonb:=to_jsonb(NEW);
BEGIN
 FOREACH key IN ARRAY TG_ARGV LOOP
  IF v->>key IS NOT NULL THEN
   INSERT INTO pg_temp.contact_start_fixture_absence_ids VALUES(TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME,key,(v->>key)::uuid)
    ON CONFLICT DO NOTHING;
  END IF;
 END LOOP;
 RETURN NEW;
END;
$capture$;
DO $observers$
DECLARE r record;v_args text;v_count integer:=0;v_sig text;
BEGIN
 PERFORM pg_temp.contact_assert_v1(current_database() IN('cloudtms_test_clone'),'seed database');
 PERFORM set_config('request.jwt.claim.role','service_role',true);
 PERFORM pg_temp.contact_assert_v1((SELECT count(*)=1 FROM pg_temp.contact_native_seed_artifact)
  AND (SELECT value->>'as_of'=(clock_timestamp() AT TIME ZONE 'Europe/London')::date::text FROM pg_temp.contact_native_seed_artifact),
  'artifact date differs from actual London day; regenerate through real offline producer');
 FOREACH v_sig IN ARRAY ARRAY[
  'public._audit_insert(text,text,text,jsonb,jsonb,text,uuid)',
  'public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamp with time zone)',
  'public.candidate_component_prepare_atomic_v1(uuid,text,uuid,integer,jsonb,text,timestamp with time zone)',
  'public.weekly_source_candidate_self_submit_atomic_v1(uuid,text,jsonb,timestamp with time zone)',
  'private._weekly_source_settings_ensure_open_cycle_v1(uuid,timestamp with time zone)',
  'public.weekly_source_upload_context_v1(jsonb)',
  'public.weekly_source_upload_stage_begin_atomic_v1(jsonb)',
  'public.weekly_source_upload_stage_rows_atomic_v1(jsonb)',
  'public.weekly_source_upload_seal_atomic_v1(jsonb)',
  'public.weekly_source_projection_begin_atomic_v1(jsonb)',
  'public.weekly_source_projection_rows_apply_atomic_v1(uuid,uuid,jsonb)',
  'public.weekly_source_projection_publish_atomic_v1(jsonb)',
  'public.weekly_source_query_ask_candidate_atomic_v1(jsonb)',
  'public.weekly_source_combined_review_workspace_v1(jsonb)',
  'private.weekly_contact_office_rows_v1(jsonb,jsonb,uuid)',
  'public.weekly_source_query_send_manager_now_atomic_v1(jsonb)',
  'public.weekly_source_global_settings_get_v1(jsonb)',
  'public.weekly_source_global_settings_save_atomic_v1(jsonb)'
 ] LOOP
  PERFORM pg_temp.contact_assert_v1(to_regprocedure(v_sig) IS NOT NULL,'missing genuine prerequisite '||v_sig);
 END LOOP;
 -- All UUID identity/unique columns in these bounded native domains are captured,
 -- including real audit/mutation receipts and children created by native triggers.
 -- No table scan, xmin guess, secret read, history rewrite or outcome injection.
 FOR r IN
  SELECT n.nspname,c.relname,array_agg(DISTINCT a.attname ORDER BY a.attname) columns
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  JOIN pg_constraint k ON k.conrelid=c.oid AND k.contype IN('p','u')
  JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=ANY(k.conkey) AND a.atttypid='uuid'::regtype
  WHERE c.relkind='r' AND n.nspname IN('public','private')
   AND(c.relname~'^(candidate_|weekly_|timesheet|contract_|summary_)'
    OR(n.nspname='public' AND c.relname IN('tms_users','clients','client_settings','candidates','contracts','audit_events')))
  GROUP BY n.nspname,c.relname ORDER BY n.nspname,c.relname
 LOOP
  v_count:=v_count+1;
  IF v_count>250 THEN RAISE EXCEPTION 'CONTACT_SEED_OBSERVER_DOMAIN_EXCEEDED';END IF;
  PERFORM pg_temp.contact_assert_v1(NOT EXISTS(SELECT 1 FROM pg_trigger t
   WHERE t.tgrelid=format('%I.%I',r.nspname,r.relname)::regclass AND t.tgname='contact_seed_insert_observer'),
   'observer name already exists');
  SELECT string_agg(quote_literal(x),',') INTO v_args FROM unnest(r.columns) x;
  EXECUTE format('CREATE TRIGGER contact_seed_insert_observer AFTER INSERT ON %I.%I FOR EACH ROW EXECUTE FUNCTION pg_temp.contact_seed_capture_insert_v1(%s)',r.nspname,r.relname,v_args);
  INSERT INTO pg_temp.contact_seed_observers VALUES(r.nspname||'.'||r.relname,'contact_seed_insert_observer');
 END LOOP;
 PERFORM pg_temp.contact_assert_v1(v_count BETWEEN 20 AND 250,'genuine native schema inventory required');
 INSERT INTO pg_temp.contact_seed_receipts VALUES('observer_catalog',jsonb_build_object('relations',v_count,'mode','AFTER_INSERT_UUID_OBSERVATION_ONLY'));
END;
$observers$;
