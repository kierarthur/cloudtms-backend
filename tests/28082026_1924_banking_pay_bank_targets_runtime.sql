\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='20s';
SET LOCAL client_min_messages='warning';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
DO $bank_targets$
DECLARE v_candidate uuid:='10000000-0000-4000-8000-000000009903';v_other uuid:='10000000-0000-4000-8000-000000009904';
 v_umbrella uuid:='10000000-0000-4000-8000-000000009901';v_case uuid:='10000000-0000-4000-8000-000000009902';
 v_candidate_hash text;v_umbrella_hash text;v_oneoff_hash text;v_target jsonb;v_targets jsonb;v_fact jsonb;v_before text;v_after text;v_error text;v_count integer;
BEGIN
 -- All records and bank values below are synthetic, disposable and rolled back.
 INSERT INTO public.umbrellas(id,name,enabled,sort_code,account_number)
 VALUES(v_umbrella,'Disposable bank target fixture',true,'00-00-00','00000000')
 RETURNING bank_details_hash INTO v_umbrella_hash;
 INSERT INTO public.candidates(id,display_name,tms_ref,umbrella_id,pay_method,account_holder,sort_code,account_number)
 VALUES(v_candidate,'Disposable bank owner A','BANK-OWNER-A',v_umbrella,'UMBRELLA','Unusable fixture A','00-00-00','00000000'),
   (v_other,'Disposable bank owner B','BANK-OWNER-B',v_umbrella,'UMBRELLA','Unusable fixture B','00-00-00','00000000');
 SELECT bank_details_hash INTO v_candidate_hash FROM public.candidates WHERE id=v_candidate;
 v_target:=jsonb_build_object('candidate_id',v_candidate,'entity_kind','CANDIDATE','entity_id',v_candidate,'bank_details_hash',v_candidate_hash);
 SELECT facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_target_facts_v2(jsonb_build_array(v_target),'REVOLUT','SANDBOX');
 IF v_fact->'target_is_current' IS DISTINCT FROM 'true'::jsonb OR v_fact->>'target_source'<>'CANDIDATE_CURRENT'
   OR v_fact->'name_check_exists'<>'false'::jsonb OR v_fact->'mapping_present'<>'false'::jsonb THEN RAISE EXCEPTION 'CANDIDATE_BANK_FACTS'; END IF;
 INSERT INTO public.bank_name_checks(rail_provider,rail_env,entity_kind,entity_id,bank_details_hash,status,checked_at_utc)
 VALUES('REVOLUT','SANDBOX','CANDIDATE',v_candidate,v_candidate_hash,'NEAR_MATCH',now());
 INSERT INTO public.bank_payee_map(rail_provider,rail_env,entity_kind,entity_id,bank_details_hash,payee_id)
 VALUES('REVOLUT','SANDBOX','CANDIDATE',v_candidate,v_candidate_hash,'UNUSABLE_SYNTHETIC_PROVIDER_REFERENCE');
 SELECT facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_target_facts_v2(jsonb_build_array(v_target),'REVOLUT','SANDBOX');
 IF v_fact->>'name_check_status'<>'NEAR_MATCH' OR v_fact->'override_current'<>'false'::jsonb
   OR v_fact->'mapping_present'<>'true'::jsonb OR length(v_fact->>'name_check_version')<>64 THEN RAISE EXCEPTION 'CHECK_AND_MAPPING_FACTS'; END IF;
 v_before:=v_fact->>'name_check_version';
 BEGIN
   UPDATE public.bank_name_checks SET override_reason='Synthetic fixture reason',override_hash='wrong-bank-hash'
   WHERE entity_id=v_candidate;
   RAISE EXCEPTION 'WRONG_HASH_OVERRIDE_STORAGE_ACCEPTED';
 EXCEPTION WHEN check_violation THEN
   GET STACKED DIAGNOSTICS v_error=CONSTRAINT_NAME;
   IF v_error<>'bank_name_checks_override_hash_chk' THEN RAISE; END IF;
 END;
 SELECT facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_target_facts_v2(jsonb_build_array(v_target),'REVOLUT','SANDBOX');
 IF v_fact->'override_current'<>'false'::jsonb OR v_before IS DISTINCT FROM v_fact->>'name_check_version' THEN RAISE EXCEPTION 'WRONG_HASH_OVERRIDE_OR_VERSION'; END IF;
 UPDATE public.bank_name_checks SET override_reason='Synthetic fixture reason',override_hash=v_candidate_hash,updated_at_utc=now()+interval '1 second' WHERE entity_id=v_candidate;
 SELECT facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_target_facts_v2(jsonb_build_array(v_target),'REVOLUT','SANDBOX');
 IF v_fact->'override_current'<>'true'::jsonb OR v_before=v_fact->>'name_check_version' THEN RAISE EXCEPTION 'EXACT_OVERRIDE_LOST'; END IF;
 SELECT facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_target_facts_v2(jsonb_build_array(v_target),'REVOLUT','PROD');
 IF v_fact->'name_check_exists'<>'false'::jsonb OR v_fact->'mapping_present'<>'false'::jsonb THEN RAISE EXCEPTION 'CROSS_ENV_BANK_FACTS'; END IF;
 SELECT facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_target_facts_v2(jsonb_build_array(v_target),'CSV','SANDBOX');
 IF v_fact->'name_check_exists'<>'false'::jsonb OR v_fact->'mapping_present'<>'false'::jsonb THEN RAISE EXCEPTION 'CROSS_PROVIDER_BANK_FACTS'; END IF;
 SELECT facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_target_facts_v2(jsonb_build_array(v_target || '{"bank_details_hash":"stale-hash"}'),'REVOLUT','SANDBOX');
 IF v_fact->'target_is_current'<>'false'::jsonb OR v_fact->'name_check_exists'<>'false'::jsonb THEN RAISE EXCEPTION 'STALE_HASH_TARGET'; END IF;
 SELECT facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_target_facts_v2(jsonb_build_array(v_target || jsonb_build_object('candidate_id',v_other)),'REVOLUT','SANDBOX');
 IF v_fact->'owner_link_valid'<>'false'::jsonb OR v_fact->'target_is_current'<>'false'::jsonb THEN RAISE EXCEPTION 'WRONG_CANDIDATE_OWNER'; END IF;
 v_target:=jsonb_build_object('candidate_id',v_candidate,'entity_kind','UMBRELLA','entity_id',v_umbrella,'bank_details_hash',v_umbrella_hash);
 SELECT facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_target_facts_v2(jsonb_build_array(v_target),'REVOLUT','SANDBOX');
 IF v_fact->>'target_source'<>'UMBRELLA_CURRENT' OR v_fact->'target_is_current'<>'true'::jsonb OR v_fact->'umbrella_enabled'<>'true'::jsonb THEN RAISE EXCEPTION 'UMBRELLA_TARGET'; END IF;
 UPDATE public.umbrellas SET enabled=false WHERE id=v_umbrella;
 SELECT facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_target_facts_v2(jsonb_build_array(v_target),'REVOLUT','SANDBOX');
 IF v_fact->'umbrella_enabled'<>'false'::jsonb OR v_fact->'target_is_current'<>'true'::jsonb THEN RAISE EXCEPTION 'INACTIVE_SEPARATE_FROM_TARGET'; END IF;
 INSERT INTO public.candidates(id,display_name,tms_ref,umbrella_id,pay_method)
 SELECT ('10000000-0000-4000-8000-'||lpad((12000+n)::text,12,'0'))::uuid,'Shared owner fixture '||n,'SHARED-BANK-'||n,v_umbrella,'UMBRELLA'
 FROM generate_series(1,103) n;
 SELECT jsonb_agg(jsonb_build_object('candidate_id',c.id,'entity_kind','UMBRELLA','entity_id',v_umbrella,'bank_details_hash',v_umbrella_hash))
 INTO v_targets FROM public.candidates c WHERE c.umbrella_id=v_umbrella;
 SELECT count(*) INTO v_count FROM private.pay_workbench_modal_bank_target_facts_v2(v_targets||v_targets,'REVOLUT','SANDBOX') t
 WHERE t.facts->'target_is_current'='true'::jsonb;
 IF v_count<>105 THEN RAISE EXCEPTION 'SHARED_UMBRELLA_TARGETS_LOST_OR_DUPLICATED'; END IF;
 SELECT facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_target_facts_v2(
   jsonb_build_array(v_target||'{"entity_id":"not-an-id"}'),'REVOLUT','SANDBOX');
 IF v_fact->'owner_exists'<>'false'::jsonb OR v_fact->'target_is_current'<>'false'::jsonb THEN RAISE EXCEPTION 'UNKNOWN_BANK_OWNER_ACCEPTED'; END IF;
 -- Actual existing one-off case/view authority, not a mocked permit.
 INSERT INTO public.pay_advances(id,candidate_id,reason,original_amount,outstanding_amount,schedule_json,status,created_at,updated_at,
   case_type,taxability,routing_kind,oneoff_bank_details_required)
 VALUES(v_case,v_candidate,'MANUAL_ADVANCE',10,10,'{}','ACTIVE',now(),now(),'PAYMENT_ADVANCE','NON_TAXABLE','ONE_OFF_SPECIFIED_BANK_ACCOUNT',true);
 INSERT INTO public.pay_finance_case_oneoff_payout_bank_details(finance_case_id,candidate_id,beneficiary_name,sort_code,account_number,bank_details_hash)
 VALUES(v_case,v_candidate,'Unusable local fixture','00-00-00','00000000','fixture-oneoff-hash') RETURNING bank_details_hash INTO v_oneoff_hash;
 IF NOT EXISTS(SELECT 1 FROM public.v_finance_cases_register WHERE finance_case_id=v_case AND edit_bank_details_allowed IS TRUE) THEN RAISE EXCEPTION 'ONEOFF_FIXTURE_NOT_EDITABLE'; END IF;
 v_target:=jsonb_build_object('candidate_id',v_candidate,'entity_kind','CANDIDATE','entity_id',v_candidate,'bank_details_hash',v_oneoff_hash);
 SELECT facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_target_facts_v2(jsonb_build_array(v_target),'REVOLUT','SANDBOX');
 IF v_fact->>'target_source'<>'CANDIDATE_ONEOFF_PAYOUT' OR v_fact->'target_is_current'<>'true'::jsonb THEN RAISE EXCEPTION 'ONEOFF_TARGET_LOST'; END IF;
 UPDATE public.pay_advances SET payout_status='PAID' WHERE id=v_case;
 SELECT facts INTO STRICT v_fact FROM private.pay_workbench_modal_bank_target_facts_v2(jsonb_build_array(v_target),'REVOLUT','SANDBOX');
 IF v_fact->'target_is_current'<>'false'::jsonb THEN RAISE EXCEPTION 'CLOSED_ONEOFF_TARGET_ACCEPTED'; END IF;
 SELECT md5(jsonb_agg(to_jsonb(n) ORDER BY n.id)::text) INTO v_before FROM public.bank_name_checks n WHERE n.entity_id=v_candidate;
 PERFORM * FROM private.pay_workbench_modal_bank_target_facts_v2(jsonb_build_array(v_target,v_target),'REVOLUT','SANDBOX');
 SELECT md5(jsonb_agg(to_jsonb(n) ORDER BY n.id)::text) INTO v_after FROM public.bank_name_checks n WHERE n.entity_id=v_candidate;
 IF v_before IS DISTINCT FROM v_after THEN RAISE EXCEPTION 'BANK_FACT_READ_WROTE'; END IF;
 BEGIN
   PERFORM * FROM private.pay_workbench_modal_bank_target_facts_v2('[false]','REVOLUT','SANDBOX');
   RAISE EXCEPTION 'INVALID_TARGET_SHAPE_ACCEPTED';
 EXCEPTION WHEN SQLSTATE '22023' THEN NULL; END;
 SET LOCAL client_min_messages='notice';
 RAISE NOTICE 'PASS: bank target exact-owner/hash/provider/environment; separate check/override/map states; actual one-off edit gate; no read mutation.';
END;
$bank_targets$;
ROLLBACK;
