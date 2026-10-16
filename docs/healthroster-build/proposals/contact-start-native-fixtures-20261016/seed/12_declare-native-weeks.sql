-- Honest TEST base configuration/accounts/planned weeks, then actual native
-- Candidate workflow -> component prepare/complete -> SELF_SUBMIT constructors.
-- A PNG spelling TEST is local synthetic evidence; no R2/provider proof is claimed.
DO $declare$
DECLARE a jsonb;v_now timestamptz:=transaction_timestamp();v_week date;v_date date;i integer;
 v_candidate uuid;v_contract uuid;v_account uuid;v_session uuid;v_workflow uuid;v_cw uuid;v_component uuid;
 v_create jsonb;v_prepare jsonb;v_complete jsonb;v_submit jsonb;v_policy jsonb;v_days jsonb;v_cycle uuid;
 v_actor uuid:='f716c000-0000-4000-8000-000000000003';v_get jsonb;v_saved jsonb;
BEGIN
 SELECT value INTO STRICT a FROM pg_temp.contact_native_seed_artifact;
 v_week:=(a->>'week_ending')::date;v_date:=(a->>'work_date')::date;
 PERFORM pg_temp.contact_assert_v1(v_date<(clock_timestamp() AT TIME ZONE 'Europe/London')::date,'worked day must be complete');
 PERFORM pg_temp.contact_assert_v1(EXISTS(SELECT 1 FROM public.settings_defaults WHERE id=1
  AND candidate_manager_email_templates_sha256 IS NOT NULL AND candidate_home_announcement_sha256 IS NOT NULL),
  'actual baseline settings/template authorities required; no fake template digest fallback');
 INSERT INTO pg_temp.contact_seed_receipts VALUES('before_shared_settings',jsonb_build_object(
  'settings_defaults_sha256',(SELECT encode(extensions.digest(convert_to(jsonb_build_object('candidate_app_feature_flags_json',x.candidate_app_feature_flags_json,'updated_at',to_jsonb(x)->'updated_at')::text,'UTF8'),'sha256'),'hex') FROM public.settings_defaults x WHERE id=1),
  'global_contact_settings_sha256',(SELECT encode(extensions.digest(convert_to((to_jsonb(x)-ARRAY['automatic_reconcile_actor_user_id','manager_first_delay_value','manager_first_delay_unit','manager_followups','manager_recent_completed_weeks','manager_max_work_age_days'])::text,'UTF8'),'sha256'),'hex') FROM public.weekly_source_global_settings x WHERE singleton)));
 UPDATE public.settings_defaults SET candidate_app_feature_flags_json=coalesce(candidate_app_feature_flags_json,'{}'::jsonb)
  ||jsonb_build_object('candidate_app_reads',true,'candidate_app_writes',true) WHERE id=1;
 INSERT INTO public.tms_users(id,email,role,password_hash,display_name,is_active) VALUES
  (v_actor,'contact-native-office@example.invalid','admin','SYNTHETIC-NO-LOGIN','Contact native fixture admin',true);
 INSERT INTO public.clients(id,name,ts_queries_email) VALUES
  ('f716c000-0000-4000-8000-000000000020','Contact Native Fixture Trust','contact-manager@example.invalid');
 INSERT INTO public.client_settings(id,client_id,effective_from,default_submission_mode,week_ending_weekday,
  hr_validation_required,autoprocess_hr,self_bill_no_invoices_sent,no_timesheet_required,requires_hr,is_nhsp)
 VALUES('f716c000-0000-4000-8000-000000000021','f716c000-0000-4000-8000-000000000020',(a->>'contract_start')::date,
  'ELECTRONIC',0,true,true,true,true,false,false);
 INSERT INTO public.weekly_source_groups(id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time)
 VALUES('f716c000-0000-4000-8000-000000000001','TEST','f7160000-0000-4000-8000-000000000019',
  'CONTACT_START_NATIVE_FIXTURE','Contact native fixture Roster','ROSTER',extract(dow FROM v_now AT TIME ZONE 'Europe/London')::integer,'23:59');
 INSERT INTO public.weekly_source_group_clients(source_group_id,client_id,valid_from,created_by_user_id)
 VALUES('f716c000-0000-4000-8000-000000000001','f716c000-0000-4000-8000-000000000020',(a->>'contract_start')::date,v_actor);
 INSERT INTO public.weekly_source_client_policies(source_group_id,client_id,effective_from,authority_mode,document_mode,
  self_bill_enabled,candidate_queries_enabled,manager_queries_enabled,manager_query_recipient,created_by_user_id)
 VALUES('f716c000-0000-4000-8000-000000000001','f716c000-0000-4000-8000-000000000020',(a->>'contract_start')::date,
  'SOURCE_AUTHORITY','CHECK_ONLY',true,true,true,'contact-manager@example.invalid',v_actor);
 -- Actual native global CAS/receipt, scoped to this rollback. Preserve all other
 -- timing choices and use a real 60 minute worker question window, no clock edits.
 v_get:=public.weekly_source_global_settings_get_v1(jsonb_build_object('actor_user_id',v_actor,
  'agency_id','f7160000-0000-4000-8000-000000000019','environment','TEST'));
 v_saved:=public.weekly_source_global_settings_save_atomic_v1(jsonb_build_object('actor_user_id',v_actor,
  'agency_id','f7160000-0000-4000-8000-000000000019','environment','TEST','request_id',gen_random_uuid(),
  'expected_settings_version',v_get->'settings_version','settings',v_get->'settings'||jsonb_build_object(
   'candidate_reminder_minutes',10,'candidate_response_deadline_minutes',60,'automatic_reconcile_actor_user_id',v_actor)));
 INSERT INTO pg_temp.contact_seed_receipts VALUES('native_global_settings',v_saved);
 v_cycle:=private._weekly_source_settings_ensure_open_cycle_v1('f716c000-0000-4000-8000-000000000001',v_now);
 PERFORM pg_temp.contact_assert_v1(EXISTS(SELECT 1 FROM public.weekly_source_cycles WHERE id=v_cycle
  AND finalisation_week_ending=v_week AND state='OPEN' AND projection_state='NONE'
  AND current_complete_upload_id IS NULL AND cutoff_at_utc>clock_timestamp()+interval '5 minutes'),
  'real open-cycle owner must allocate expected week with future cutoff');
 INSERT INTO pg_temp.contact_seed_receipts VALUES('native_cycle',jsonb_build_object('source_cycle_id',v_cycle));
 SELECT jsonb_agg(to_char(d,'YYYY-MM-DD') ORDER BY d) INTO v_days
 FROM generate_series((a->>'week_start')::date::timestamp,v_week::timestamp,interval '1 day') d WHERE d::date<>v_date;
 FOR i IN 0..2 LOOP
  v_candidate:=('f716c000-0000-4000-8000-'||lpad((10+i)::text,12,'0'))::uuid;
  v_contract:=('f716c000-0000-4000-8000-'||lpad((30+i)::text,12,'0'))::uuid;
  v_account:=('f716c000-0000-4000-8000-'||lpad((40+i)::text,12,'0'))::uuid;
  v_session:=('f716c000-0000-4000-8000-'||lpad((50+i)::text,12,'0'))::uuid;
  v_workflow:=('f716c000-0000-4000-8000-'||lpad((70+i)::text,12,'0'))::uuid;
  v_cw:=('f716c000-0000-4000-8000-'||lpad((90+i)::text,12,'0'))::uuid;
  INSERT INTO public.candidates(id,tms_ref,first_name,last_name,display_name,email,active,key_norm,opt_in_email,nhsp_hr_name_aliases)
  VALUES(v_candidate,'CONTACT-TEST-'||chr(65+i),'Contact Worker',chr(65+i),'Contact Worker '||chr(65+i),
   'contact-worker-'||lower(chr(65+i))||'@example.invalid',true,'CONTACT-NATIVE-'||chr(65+i),true,jsonb_build_array('Contact Worker '||chr(65+i)));
  INSERT INTO public.contracts(id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
   week_ending_weekday_snapshot,default_submission_mode,role)
  VALUES(v_contract,v_candidate,'f716c000-0000-4000-8000-000000000020',(a->>'contract_start')::date,(a->>'contract_end')::date,
   'PAYE','{}'::jsonb,0,'ELECTRONIC','RMN');
  -- Planned/open Contract Week is a synthetic scheduling fact, as in the actual
  -- mandatory SELF_SUBMIT verifier. No Timesheet, current owner, approval or finance is seeded.
  INSERT INTO public.contract_weeks(id,contract_id,week_ending_date,status,submission_mode_snapshot,day_entries_json,totals_json)
  VALUES(v_cw,v_contract,v_week,'OPEN','ELECTRONIC','[]'::jsonb,'{}'::jsonb);
  INSERT INTO public.candidate_app_accounts(id,environment,email_normalized,status,password_scheme,password_scheme_version,
   password_salt,password_digest,password_changed_at_utc)
  VALUES(v_account,'TEST','contact-worker-'||lower(chr(65+i))||'@example.invalid','ACTIVE','PBKDF2-HMAC-SHA256',1,
   extensions.digest(convert_to('SYNTHETIC-NO-LOGIN-SALT-'||i,'UTF8'),'sha256'),
   extensions.digest(convert_to('SYNTHETIC-NO-LOGIN-DIGEST-'||i,'UTF8'),'sha256'),v_now);
  INSERT INTO public.candidate_app_sessions(id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
   expires_at_utc,absolute_expires_at_utc)
  VALUES(v_session,v_account,'TEST',v_candidate,'ACTIVE',extensions.digest(convert_to('SYNTHETIC-NO-TOKEN-'||i,'UTF8'),'sha256'),
   v_now+interval '2 hours',v_now+interval '3 hours');
  INSERT INTO public.candidate_app_global_membership_links(membership_id,global_account_identity_hmac,account_id,candidate_id,
   candidate_code,membership_generation,state,linked_at_utc,updated_at_utc)
  VALUES(('f716c000-0000-4000-8000-'||lpad((60+i)::text,12,'0'))::uuid,
   extensions.digest(convert_to('SYNTHETIC-NO-CONTROL-PLANE-'||i,'UTF8'),'sha256'),v_account,v_candidate,'CONTACT-TEST-'||chr(65+i),1,'ACTIVE',v_now,v_now);
  v_policy:=private._weekly_source_effective_policy_v1('f716c000-0000-4000-8000-000000000020',v_contract,v_date);
  PERFORM pg_temp.contact_assert_v1(v_policy->>'authority_mode'='SOURCE_AUTHORITY' AND v_policy->>'document_mode'='CHECK_ONLY',
   'native effective source policy required');
  v_create:=public.candidate_workflow_transition_atomic_v1(v_session,'TEST',v_workflow,'CREATE',NULL,
   jsonb_build_object('workflow_kind','CONTRACT_HOURS','scope','WEEKLY','route','ELECTRONIC','contract_id',v_contract,
    'contract_week_id',v_cw,'week_ending_date',v_week,'input_snapshot','{}'::jsonb),'contact-native-create-'||i,v_now);
  PERFORM pg_temp.contact_assert_v1(v_create->>'state'='WORKER_DRAFT' AND v_create->>'generation'='1','native draft CREATE');
  v_prepare:=public.candidate_component_prepare_atomic_v1(v_session,'TEST',v_workflow,1,
   jsonb_build_object('component_kind','CANDIDATE_SIGNATURE','document_role','CANDIDATE_SIGNATURE','media_type','image/png',
    'byte_size',a->'signature'->'byte_count','storage_key',a->'signature'->>'storage_key'),'contact-native-component-'||i,v_now);
  v_component:=(v_prepare->>'component_id')::uuid;
  PERFORM pg_temp.contact_assert_v1(v_component IS NOT NULL AND v_prepare->>'state'='PENDING','native signature PREPARE');
  v_complete:=public.candidate_workflow_transition_atomic_v1(v_session,'TEST',v_workflow,'COMPONENT_COMPLETE',1,
   jsonb_build_object('component_id',v_component,'source_content_sha256_hex',a->'signature'->>'sha256',
    'verified_byte_size',a->'signature'->'byte_count','verified_media_type','image/png',
    'verified_image_width',a->'signature'->'width','verified_image_height',a->'signature'->'height'),'contact-native-complete-'||i,v_now);
  PERFORM pg_temp.contact_assert_v1(EXISTS(SELECT 1 FROM public.candidate_submission_components WHERE id=v_component
   AND state='IMMUTABLE' AND encode(source_content_sha256,'hex')=a->'signature'->>'sha256'), 'native COMPLETE immutable fixture bytes');
  v_submit:=public.weekly_source_candidate_self_submit_atomic_v1(v_session,'TEST',jsonb_build_object('workflow_id',v_workflow,
   'expected_workflow_generation',1,'candidate_signature_component_id',v_component,'candidate_signed_at_utc',v_now,
   'immutable_submission',jsonb_build_object('actual_schedule_json',jsonb_build_array(jsonb_build_object('row_key','contact-fixture-'||i,
    'date',v_date,'start','09:00','end','17:00','break_minutes',30)),'day_off_dates',v_days),
   'idempotency_key',gen_random_uuid()),v_now);
  PERFORM pg_temp.contact_assert_v1(v_submit->>'request_kind'='SELF_SUBMIT' AND v_submit->>'generation'='2'
   AND v_submit->>'timesheet_id' IS NOT NULL,'real signed whole-week declaration');
  PERFORM pg_temp.contact_assert_v1(EXISTS(SELECT 1 FROM public.timesheets t WHERE t.timesheet_id=(v_submit->>'timesheet_id')::uuid
   AND t.contract_id=v_contract AND t.candidate_workflow_id=v_workflow AND t.authorised_at_server IS NULL
   AND t.r2_nurse_key=a->'signature'->>'storage_key' AND t.img_sha256_nurse=a->'signature'->>'sha256'
   AND t.is_current AND t.archived_at_utc IS NULL AND t.revoked_at IS NULL)
   AND NOT EXISTS(SELECT 1 FROM public.timesheets_financials WHERE timesheet_id=(v_submit->>'timesheet_id')::uuid),
   'actual declaration must be unapproved, signed and nonfinancial');
  INSERT INTO pg_temp.contact_seed_receipts VALUES('declaration_'||chr(65+i),jsonb_build_object(
   'create',v_create,'prepare',v_prepare-'storage_key','complete',v_complete,'self_submit',v_submit));
 END LOOP;
END;
$declare$;
