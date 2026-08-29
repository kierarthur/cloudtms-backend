\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='25s';
SET LOCAL client_min_messages='warning';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
DO $issues$
DECLARE v_session public.banking_pay_workbench_sessions%ROWTYPE;
 v_umbrella uuid:='10000000-0000-4000-8000-000000009991';v_hash text;v_count integer;v_keys integer;
 v_candidate uuid:='10000000-0000-4000-8000-000000020001';v_other uuid:='10000000-0000-4000-8000-000000020002';
 v_seq bigint;v_payee jsonb;v_payload jsonb;v_task text;v_changed text;v_before text;v_after text;
 v_options jsonb;v_detail jsonb;v_members jsonb;v_cursor text;
BEGIN
 SELECT * INTO STRICT v_session FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 INSERT INTO public.umbrellas(id,name,enabled,sort_code,account_number)
 VALUES(v_umbrella,'Disposable shared bank issue fixture',true,'00-00-00','00000000') RETURNING bank_details_hash INTO v_hash;
 INSERT INTO public.candidates(id,display_name,tms_ref,umbrella_id,pay_method)
 SELECT ('10000000-0000-4000-8000-'||lpad((20000+n)::text,12,'0'))::uuid,'Bank issue fixture '||n,'BANK-ISSUE-'||n,v_umbrella,'UMBRELLA'
 FROM generate_series(1,105) n;
 INSERT INTO public.banking_pay_workbench_session_scope(session_id,candidate_id,scope_ordinal,status,seeded,dirty)
 SELECT v_session.id,('10000000-0000-4000-8000-'||lpad((20000+n)::text,12,'0'))::uuid,100+n,'READY',true,false FROM generate_series(1,105) n;
 INSERT INTO public.banking_pay_workbench_preview_rows(id,session_id,candidate_id,section,row_key,row_ordinal,row_json,key_type,key_value,selected,selection_state,status,session_version)
 SELECT ('10000000-0000-4000-8000-'||lpad((21000+n)::text,12,'0'))::uuid,v_session.id,
 ('10000000-0000-4000-8000-'||lpad((20000+n)::text,12,'0'))::uuid,'canonical_preview_lines','bank-issue:'||n,100+n,
 '{"pay_channel":"UMBRELLA","amount_display":"10.00","section_amount_display":"10.00","line_type":"TIMESHEET_PAYMENT","selection_allowed":true,"draftable":true,"is_ready_for_draft":true}',
 'SOURCE_REF','bank-issue:'||n,true,'SELECTED','READY',v_session.version FROM generate_series(1,105) n;
 INSERT INTO public.banking_pay_workbench_session_candidate_state(session_id,candidate_id,session_version,status,effective_candidate_fragment_json,effective_payees_json)
 SELECT v_session.id,c.id,v_session.version,'READY',jsonb_build_object('candidate_id',c.id,'current_pay_method','UMBRELLA'),
 jsonb_build_array(jsonb_build_object('candidate_id',c.id,'payee_entity_kind','UMBRELLA','payee_entity_id',v_umbrella,
   'bank_details_hash',v_hash,'pay_channel','UMBRELLA','blockers',jsonb_build_array('BLOCKED_NAME_CHECK'),'name_check_status','NEAR_MATCH'))
 FROM public.candidates c WHERE c.umbrella_id=v_umbrella;
 INSERT INTO public.bank_name_checks(rail_provider,rail_env,entity_kind,entity_id,bank_details_hash,status,checked_at_utc)
 VALUES('REVOLUT','SANDBOX','UMBRELLA',v_umbrella,v_hash,'NEAR_MATCH',now());
 SELECT count(*),count(DISTINCT task_key),min(task_key) INTO v_count,v_keys,v_task
 FROM private.pay_workbench_modal_bank_issue_members_v2(v_session,'ALL','REVOLUT','SANDBOX',true)
 WHERE task_json->>'state'='ACTION_REQUIRED';
 IF v_count<>105 OR v_keys<>1 THEN RAISE EXCEPTION 'SHARED_BANK_TASK_GROUPING: %/%',v_count,v_keys;END IF;
 SELECT count(*),count(DISTINCT identity) INTO v_count,v_keys
 FROM private.pay_workbench_modal_issue_index_v2(v_session,'ALL','REVOLUT','SANDBOX','{"next_recommended_action":"WAIT_FOR_WORKER"}',true,true);
 IF v_count<>4 OR v_keys<>4 THEN RAISE EXCEPTION 'SHARED_BANK_INDEX_COUNT: %/%',v_count,v_keys;END IF;
 SELECT count(*),count(DISTINCT task_key) INTO v_count,v_keys
 FROM private.pay_workbench_modal_bank_task_members_v2(v_session,'ALL','REVOLUT','SANDBOX',true);
 IF v_count<>210 OR v_keys<>1 THEN RAISE EXCEPTION 'BANK_TASK_DID_NOT_RETAIN_ALL_CANDIDATE_CONTEXT: %/%',v_count,v_keys;END IF;
 UPDATE public.settings_defaults SET rail_provider_default='REVOLUT',rail_env_default='SANDBOX' WHERE id=1;
 v_options:=jsonb_build_object('expected_session_version',v_session.version,'expected_progress_counter_version',v_session.progress_counter_version,
   'scope_hash',private.pay_workbench_modal_scope_hash_v2(v_session,'ALL'),'pay_channel_scope','ALL');
 v_detail:=public.pay_workbench_session_get_action_required_detail_v1(v_session.id,v_options,v_session.actor_user_id,v_task,NULL,100);
 IF v_detail->>'total_count'<>'210' OR v_detail->>'affected_candidate_count'<>'105'
  OR v_detail->'affected_payment_count_complete' IS DISTINCT FROM 'false'::jsonb OR v_detail->'affected_payment_count'<>'null'::jsonb THEN
  RAISE EXCEPTION 'PUBLIC_SHARED_BANK_UNKNOWN_COUNT_OR_MEMBERSHIP';END IF;
 v_members:=v_detail->'rows';
 WHILE v_detail->>'has_more'='true' LOOP
  v_detail:=public.pay_workbench_session_get_action_required_detail_v1(v_session.id,v_options,v_session.actor_user_id,v_task,v_detail->>'next_cursor',100);
  v_members:=v_members||(v_detail->'rows');
 END LOOP;
 IF jsonb_array_length(v_members)<>210 OR (SELECT count(DISTINCT m->>'identity') FROM jsonb_array_elements(v_members) m)<>210 THEN
  RAISE EXCEPTION 'PUBLIC_SHARED_BANK_LOST_MEMBER';END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_members) m WHERE m->>'source_kind'='STORED_PAYEE'
   AND (m->'preview_row_id'<>'null'::jsonb OR m#>>'{task_meta,action}'<>'banking:pay:acceptBankDetails')) THEN
  RAISE EXCEPTION 'PUBLIC_SHARED_BANK_LOST_ACTION_OR_INVENTED_PAYMENT';END IF;
 IF EXISTS(SELECT 1 FROM private.pay_workbench_modal_bank_task_members_v2(v_session,'ALL','REVOLUT','SANDBOX',true)
   WHERE source_kind='PREVIEW_ROW' AND (NOT context_only OR affected_by_task OR bank_row IS NOT NULL)) THEN
   RAISE EXCEPTION 'BANK_TASK_GUESSED_UNLINKED_PAYMENT_ROUTE';END IF;
 UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json||jsonb_build_object(
   'payee_entity_kind','UMBRELLA','payee_entity_id',v_umbrella,'bank_details_hash',v_hash)
 WHERE session_id=v_session.id AND candidate_id=v_candidate;
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_bank_task_members_v2(v_session,'ALL','REVOLUT','SANDBOX',true)
 WHERE source_kind='PREVIEW_ROW' AND affected_by_task AND context_only;
 IF v_count<>1 THEN RAISE EXCEPTION 'EXACT_BANK_ROUTE_NOT_RECOGNISED';END IF;
 -- Prove a complete count only once every current physical route is known.
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_task_summaries_v2(v_session,'ALL','REVOLUT','SANDBOX',
  '{"next_recommended_action":"WAIT_FOR_WORKER"}',true,true) t
 WHERE t.identity=v_task AND t.affected_candidate_count=105 AND t.affected_payment_count IS NULL
  AND t.affected_payment_count_complete IS FALSE AND t.title_message_id='MSG-071';
 IF v_count<>1 THEN RAISE EXCEPTION 'TASK_LIST_SHARED_BANK_UNKNOWN_COUNT';END IF;
 UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json||jsonb_build_object(
   'payee_entity_kind','UMBRELLA','payee_entity_id',v_umbrella,'bank_details_hash',v_hash)
 WHERE session_id=v_session.id AND candidate_id IN(SELECT id FROM public.candidates WHERE umbrella_id=v_umbrella);
 v_detail:=public.pay_workbench_session_get_action_required_detail_v1(v_session.id,v_options,v_session.actor_user_id,v_task,NULL,100);
 IF v_detail->'affected_payment_count_complete' IS DISTINCT FROM 'true'::jsonb OR v_detail->>'affected_payment_count'<>'105' THEN
  RAISE EXCEPTION 'PUBLIC_SHARED_BANK_COMPLETE_COUNT';END IF;
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_task_summaries_v2(v_session,'ALL','REVOLUT','SANDBOX',
  '{"next_recommended_action":"WAIT_FOR_WORKER"}',true,true) t
 WHERE t.identity=v_task AND t.affected_candidate_count=105 AND t.affected_payment_count=105
  AND t.affected_payment_count_complete IS TRUE;
 IF v_count<>1 THEN RAISE EXCEPTION 'TASK_LIST_SHARED_BANK_COMPLETE_COUNT';END IF;
 UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json-'payee_entity_kind'-'payee_entity_id'-'bank_details_hash'
 WHERE session_id=v_session.id AND candidate_id<>v_candidate AND candidate_id IN(SELECT id FROM public.candidates WHERE umbrella_id=v_umbrella);
 -- A stale member remains context in the one current account task. It cannot
 -- borrow an Accept action from a different candidate's current source.
 UPDATE public.banking_pay_workbench_session_candidate_state SET effective_payees_json=jsonb_set(effective_payees_json,'{0,name_check_status}','"PASS"')
 WHERE session_id=v_session.id AND candidate_id=v_other;
 SELECT count(*),count(DISTINCT task_key) INTO v_count,v_keys
 FROM private.pay_workbench_modal_bank_task_members_v2(v_session,'ALL','REVOLUT','SANDBOX',true)
 WHERE task_state='ACTION_REQUIRED';
 IF v_count<>210 OR v_keys<>1 THEN RAISE EXCEPTION 'MIXED_SNAPSHOT_DUPLICATED_BANK_TASK';END IF;
 IF EXISTS(SELECT 1 FROM private.pay_workbench_modal_bank_task_members_v2(v_session,'ALL','REVOLUT','SANDBOX',true)
   WHERE candidate_id=v_other AND source_kind='STORED_PAYEE' AND (NOT context_only OR task_json->>'action' IS NOT NULL)) THEN
   RAISE EXCEPTION 'STALE_BANK_MEMBER_BORROWED_ACTION';END IF;
 UPDATE public.banking_pay_workbench_session_candidate_state SET effective_payees_json=jsonb_set(effective_payees_json,'{0,name_check_status}','"NEAR_MATCH"')
 WHERE session_id=v_session.id AND candidate_id=v_other;
 IF EXISTS(SELECT 1 FROM private.pay_workbench_modal_bank_issue_members_v2(v_session,'ALL','REVOLUT','SANDBOX',true) i
  JOIN private.pay_workbench_modal_bank_sources_v2(v_session,'ALL') s ON i.candidate_id=s.candidate_id
  WHERE i.source_payload IS DISTINCT FROM s.source_payload OR i.bank_row IS DISTINCT FROM s.bank_row OR i.preview_row_id IS NOT NULL) THEN
  RAISE EXCEPTION 'BANK_TASK_LOST_PAYLOAD_OR_INVENTED_PAYMENT';END IF;
 DELETE FROM public.banking_pay_workbench_preview_rows WHERE session_id=v_session.id AND candidate_id=v_other;
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_bank_issue_members_v2(v_session,'ALL','REVOLUT','SANDBOX',true);
 IF v_count<>105 THEN RAISE EXCEPTION 'SOURCE_ONLY_TASK_LOST';END IF;
 UPDATE public.bank_name_checks SET checked_at_utc=now()+interval '1 second',updated_at_utc=now()+interval '1 second' WHERE entity_id=v_umbrella;
 SELECT min(task_key) INTO v_changed FROM private.pay_workbench_modal_bank_issue_members_v2(v_session,'ALL','REVOLUT','SANDBOX',true);
 IF v_changed=v_task THEN RAISE EXCEPTION 'NEW_BANK_RESULT_REUSED_TASK_TOKEN';END IF;
 SELECT COALESCE(seq,0) INTO v_seq FROM public.app_change_counters WHERE entity_key='pay_candidate:'||v_candidate::text;
 v_payee:=jsonb_build_array(jsonb_build_object('candidate_id',v_candidate,'payee_entity_kind','UMBRELLA','payee_entity_id',v_umbrella,'bank_details_hash',v_hash));
 v_payload:=jsonb_build_object('session_id',v_session.id,'candidate_id',v_candidate,'session_version',v_session.version,
 'source_change_seq',v_seq,'readiness_fingerprint',md5(v_payee::text),'payees_json',v_payee,'rail_env','SANDBOX');
 INSERT INTO public.banking_pay_workbench_jobs(id,job_type,status,session_id,candidate_id,snapshot_run_id,dedupe_key,payload_json)
 VALUES('10000000-0000-4000-8000-000000009952','PAYEE_READINESS_ENSURE','RUNNING',v_session.id,v_candidate,v_session.source_snapshot_run_id,'shared-bank-issue-fixture',v_payload);
 SELECT count(*),count(DISTINCT task_key) INTO v_count,v_keys FROM private.pay_workbench_modal_bank_issue_members_v2(v_session,'ALL','REVOLUT','SANDBOX',true)
 WHERE task_json->>'state'='UPDATING';
 IF v_count<>105 OR v_keys<>1 THEN RAISE EXCEPTION 'SHARED_BANK_JOB_NOT_GROUPED';END IF;
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_issue_index_v2(v_session,'ALL','REVOLUT','SANDBOX','{"next_recommended_action":"WAIT_FOR_WORKER"}',true,true)
 WHERE issue_state='UPDATING';
 IF v_count<>1 THEN RAISE EXCEPTION 'SHARED_BANK_INDEX_DUPLICATED_UPDATING';END IF;
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_task_summaries_v2(v_session,'ALL','REVOLUT','SANDBOX',
  '{"next_recommended_action":"WAIT_FOR_WORKER"}',true,true) t
 WHERE t.issue_state='UPDATING' AND t.affected_candidate_count=105 AND t.title_message_id='MSG-060';
 IF v_count<>1 THEN RAISE EXCEPTION 'TASK_LIST_SHARED_BANK_UPDATING';END IF;
 v_session.filters_json:=jsonb_build_object('candidate_id',v_other);
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_bank_issue_members_v2(v_session,'ALL','REVOLUT','SANDBOX',true)
 WHERE task_json->>'state'='UPDATING';
 IF v_count<>1 THEN RAISE EXCEPTION 'FILTER_HID_CURRENT_SHARED_BANK_JOB';END IF;
 v_session.filters_json:='{}';
 UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json||'{"presentation_role":"HIDDEN_INDEFINITE_SNOOZE"}' WHERE session_id=v_session.id AND candidate_id=v_candidate;
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_bank_issue_members_v2(v_session,'ALL','REVOLUT','SANDBOX',true);
 IF v_count<>104 THEN RAISE EXCEPTION 'HIDDEN_BANK_MEMBER_RETURNED';END IF;
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_bank_issue_members_v2(v_session,'PAYE','REVOLUT','SANDBOX',true);
 IF v_count<>0 THEN RAISE EXCEPTION 'BANK_TASK_CHANNEL_SCOPE';END IF;
 SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY id)::text) INTO v_before FROM public.banking_pay_workbench_preview_rows r WHERE session_id=v_session.id;
 PERFORM * FROM private.pay_workbench_modal_bank_issue_members_v2(v_session,'ALL','REVOLUT','SANDBOX',true);
 SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY id)::text) INTO v_after FROM public.banking_pay_workbench_preview_rows r WHERE session_id=v_session.id;
 IF v_before IS DISTINCT FROM v_after THEN RAISE EXCEPTION 'BANK_TASK_MOVED_OR_CHANGED_PAYMENT';END IF;
 INSERT INTO public.candidates(id,display_name,tms_ref,pay_method)
 SELECT ('10000000-0000-4000-8000-'||lpad((22000+n)::text,12,'0'))::uuid,'Unknown bank owner fixture '||n,'UNKNOWN-BANK-'||n,'PAYE'
 FROM generate_series(1,2) n;
 INSERT INTO public.banking_pay_workbench_session_scope(session_id,candidate_id,scope_ordinal,status,seeded,dirty)
 SELECT v_session.id,('10000000-0000-4000-8000-'||lpad((22000+n)::text,12,'0'))::uuid,1000+n,'READY',true,false FROM generate_series(1,2) n;
 INSERT INTO public.banking_pay_workbench_session_candidate_state(session_id,candidate_id,session_version,status,effective_candidate_fragment_json,effective_payees_json)
 SELECT v_session.id,('10000000-0000-4000-8000-'||lpad((22000+n)::text,12,'0'))::uuid,v_session.version,'READY',
 jsonb_build_object('candidate_id',('10000000-0000-4000-8000-'||lpad((22000+n)::text,12,'0')),'current_pay_method','PAYE'),
 jsonb_build_array(jsonb_build_object('candidate_id',('10000000-0000-4000-8000-'||lpad((22000+n)::text,12,'0')),
   'pay_channel','PAYE','blockers',jsonb_build_array('BLOCKED_NAME_CHECK'),'name_check_status','NEAR_MATCH')) FROM generate_series(1,2) n;
 SELECT count(*),count(DISTINCT task_key) INTO v_count,v_keys FROM private.pay_workbench_modal_bank_issue_members_v2(v_session,'PAYE','REVOLUT','SANDBOX',true);
 IF v_count<>2 OR v_keys<>2 THEN RAISE EXCEPTION 'UNKNOWN_BANK_OWNERS_MERGED: %/%',v_count,v_keys;END IF;
 SET LOCAL client_min_messages='notice';
 RAISE NOTICE 'PASS: shared bank problems retain 105 candidates and original sources, 210 complete source/context members, exact route versus other context, stale-member action fence, distinct unknown owners, exact result versions, cross-filter current work and hidden boundaries without changing any payment.';
END;
$issues$;
ROLLBACK;
