\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='20s';
SET LOCAL client_min_messages='warning';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
INSERT INTO public.candidates(id,display_name,tms_ref)
SELECT ('10000000-0000-4000-8000-' || lpad((6000+n)::text,12,'0'))::uuid,'Bank source fixture ' || n,'BANK-SOURCE-' || n FROM generate_series(1,103) n;
INSERT INTO public.banking_pay_workbench_session_scope(session_id,candidate_id,scope_ordinal,status,seeded,dirty)
SELECT '10000000-0000-4000-8000-000000000005'::uuid,('10000000-0000-4000-8000-' || lpad((6000+n)::text,12,'0'))::uuid,n+2,'READY',true,false
FROM generate_series(1,103) n;
INSERT INTO public.banking_pay_workbench_preview_rows(id,session_id,candidate_id,section,row_key,row_ordinal,row_json,key_type,key_value,selected,selection_state,status,session_version)
SELECT ('10000000-0000-4000-8000-' || lpad((7000+n)::text,12,'0'))::uuid,'10000000-0000-4000-8000-000000000005'::uuid,
  ('10000000-0000-4000-8000-' || lpad((6000+n)::text,12,'0'))::uuid,'canonical_preview_lines','bank-source:' || n,n+1000,
  '{"pay_channel":"UMBRELLA","amount_display":"10.00","section_amount_display":"10.00","line_type":"TIMESHEET_PAYMENT","selection_allowed":true,"draftable":true,"is_ready_for_draft":true}'::jsonb,
  'SOURCE_REF','bank-source:' || n,true,'SELECTED','READY',1 FROM generate_series(1,103) n;
UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json || '{"pay_channel":"UMBRELLA"}'
WHERE session_id='10000000-0000-4000-8000-000000000005';
INSERT INTO public.banking_pay_workbench_session_candidate_state(session_id,candidate_id,session_version,status,effective_candidate_fragment_json,effective_payees_json,effective_non_paye_payee_json)
SELECT s.session_id,s.candidate_id,1,'READY',jsonb_build_object('candidate_id',s.candidate_id,'current_pay_method','UMBRELLA'),
  jsonb_build_array(p.payee,p.payee),p.payee
FROM public.banking_pay_workbench_session_scope s
CROSS JOIN LATERAL (SELECT jsonb_build_object('candidate_id',s.candidate_id,'payee_entity_kind','UMBRELLA',
  'payee_entity_id','10000000-0000-4000-8000-000000009991','bank_details_hash','synthetic-shared-bank-hash',
  'pay_channel','UMBRELLA','blockers',jsonb_build_array('BLOCKED_NAME_CHECK'),
  'name_check_status','NEAR_MATCH','section_amount_display','10.00') AS payee) p
WHERE s.session_id='10000000-0000-4000-8000-000000000005';

CREATE FUNCTION pg_temp.bank_source_count(p_channel text,p_candidates integer,p_members integer,p_filters jsonb DEFAULT '{}')
RETURNS void LANGUAGE plpgsql AS $test$
DECLARE v_session public.banking_pay_workbench_sessions%ROWTYPE; v_count integer;v_candidates integer;v_hash text;v_after text;
BEGIN
  SELECT * INTO STRICT v_session FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
  v_session.filters_json:=p_filters;
  SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY id)::text) INTO v_hash FROM public.banking_pay_workbench_preview_rows r WHERE session_id=v_session.id;
  SELECT count(*),count(DISTINCT candidate_id) INTO v_count,v_candidates
  FROM private.pay_workbench_modal_bank_sources_v2(v_session,p_channel);
  IF v_count<>p_members OR v_candidates<>p_candidates THEN
    RAISE EXCEPTION 'BANK_SOURCE_SCOPE: channel % expected % members/% candidates got %/%',p_channel,p_members,p_candidates,v_count,v_candidates;
  END IF;
  SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY id)::text) INTO v_after FROM public.banking_pay_workbench_preview_rows r WHERE session_id=v_session.id;
  IF v_hash IS DISTINCT FROM v_after THEN RAISE EXCEPTION 'BANK_SOURCE_READ_WROTE_PAYMENTS'; END IF;
END;
$test$;
DO $sources$
DECLARE v_id uuid:='10000000-0000-4000-8000-000000000005';v_candidate uuid:='10000000-0000-4000-8000-000000000002';
  v_session public.banking_pay_workbench_sessions%ROWTYPE; v_count integer;v_error text;v_row record;
  v_saved public.banking_pay_workbench_preview_rows%ROWTYPE;
BEGIN
  PERFORM pg_temp.bank_source_count('ALL',105,105);
  PERFORM pg_temp.bank_source_count('UMBRELLA',105,105);
  PERFORM pg_temp.bank_source_count('PAYE',0,0);
  PERFORM pg_temp.bank_source_count('ALL',1,1,jsonb_build_object('candidate_id',v_candidate));
  SELECT * INTO STRICT v_session FROM public.banking_pay_workbench_sessions WHERE id=v_id;
  SELECT count(DISTINCT bank_row->>'__payee_route_key') INTO v_count FROM private.pay_workbench_modal_bank_sources_v2(v_session,'ALL');
  IF v_count<>1 THEN RAISE EXCEPTION 'SHARED_BANK_OWNER_WAS_SPLIT'; END IF;
  IF EXISTS(SELECT 1 FROM private.pay_workbench_modal_bank_sources_v2(v_session,'ALL') WHERE source_kind<>'STORED_PAYEE' OR preview_row_id IS NOT NULL) THEN
    RAISE EXCEPTION 'SYNTHETIC_SOURCE_FABRICATED_A_PAYMENT_ROW';
  END IF;
  UPDATE public.banking_pay_workbench_session_candidate_state SET
    effective_candidate_fragment_json=effective_candidate_fragment_json || '{"payee_context":{"payee_entity_kind":"UMBRELLA","payee_entity_id":"10000000-0000-4000-8000-000000009991","bank_details_hash":"synthetic-shared-bank-hash"}}',
    effective_payees_json=jsonb_build_array((effective_payees_json->0)-'payee_entity_kind'-'payee_entity_id'-'bank_details_hash')
  WHERE session_id=v_id AND candidate_id='10000000-0000-4000-8000-000000000003';
  PERFORM pg_temp.bank_source_count('ALL',105,105);
  SELECT * INTO STRICT v_row FROM private.pay_workbench_modal_bank_sources_v2(v_session,'ALL') WHERE candidate_id='10000000-0000-4000-8000-000000000003';
  IF v_row.bank_row->>'payee_entity_kind'<>'UMBRELLA' OR v_row.bank_row->>'bank_details_hash'<>'synthetic-shared-bank-hash' THEN
    RAISE EXCEPTION 'NESTED_CANDIDATE_BANK_OWNER_LOST';
  END IF;
  -- Physical evidence has precedence for this candidate/account only. Other
  -- candidates sharing the same umbrella cannot be lost by global deduplication.
  UPDATE public.banking_pay_workbench_preview_rows SET section='blocked_for_pay',row_json=row_json || jsonb_build_object(
    'blockers',jsonb_build_array('BLOCKED_NAME_CHECK'),'payee_entity_kind','UMBRELLA',
    'payee_entity_id','10000000-0000-4000-8000-000000009991','bank_details_hash','synthetic-shared-bank-hash',
    'name_check_status','FAIL','name_check_has_override',false,'payee_map_present',false)
  WHERE id='10000000-0000-4000-8000-000000001001';
  PERFORM pg_temp.bank_source_count('ALL',105,105);
  SELECT * INTO STRICT v_row FROM private.pay_workbench_modal_bank_sources_v2(v_session,'ALL') WHERE candidate_id=v_candidate;
  IF v_row.source_kind<>'PREVIEW_ROW' OR v_row.bank_row->>'name_check_status'<>'FAIL'
    OR v_row.bank_row->'name_check_has_override'<>'false'::jsonb
    OR v_row.source_payload->>'preview_row_id'<>'10000000-0000-4000-8000-000000001001' THEN RAISE EXCEPTION 'PHYSICAL_BANK_SOURCE_PRECEDENCE'; END IF;
  UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json || '{"presentation_role":"HIDDEN_INDEFINITE_SNOOZE"}'
  WHERE session_id=v_id AND candidate_id=v_candidate;
  PERFORM pg_temp.bank_source_count('ALL',104,104);
  UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json-'presentation_role'
  WHERE session_id=v_id AND candidate_id=v_candidate;
  UPDATE public.banking_pay_workbench_session_candidate_state SET session_version=2 WHERE session_id=v_id AND candidate_id='10000000-0000-4000-8000-000000000003';
  PERFORM pg_temp.bank_source_count('ALL',104,104);
  UPDATE public.banking_pay_workbench_session_candidate_state SET session_version=1 WHERE session_id=v_id AND candidate_id='10000000-0000-4000-8000-000000000003';
  UPDATE public.banking_pay_workbench_session_scope SET status='SOURCE_BUILD_PENDING' WHERE session_id=v_id AND candidate_id='10000000-0000-4000-8000-000000000003';
  PERFORM pg_temp.bank_source_count('ALL',104,104);
  UPDATE public.banking_pay_workbench_session_scope SET status='READY' WHERE session_id=v_id AND candidate_id='10000000-0000-4000-8000-000000000003';
  UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json || '{"client_id":"10000000-0000-4000-8000-000000009992"}' WHERE session_id=v_id;
  PERFORM pg_temp.bank_source_count('ALL',0,0,'{"client_id":"10000000-0000-4000-8000-000000009993"}');
  -- The legacy synthetic bank presentation does not depend on having a
  -- physical payment. Preserve a current source-only issue, without a guessed
  -- payment ID/count. This fallback must not revive hidden physical payments.
  DELETE FROM public.banking_pay_workbench_preview_rows
  WHERE id='10000000-0000-4000-8000-000000007001' RETURNING * INTO STRICT v_saved;
  PERFORM pg_temp.bank_source_count('ALL',105,105);
  SELECT * INTO STRICT v_row FROM private.pay_workbench_modal_bank_sources_v2(v_session,'ALL')
  WHERE candidate_id='10000000-0000-4000-8000-000000006001';
  IF v_row.preview_row_id IS NOT NULL OR v_row.source_kind<>'STORED_PAYEE'
    OR v_row.bank_row->>'bank_details_hash'<>'synthetic-shared-bank-hash' THEN
    RAISE EXCEPTION 'BANK_ONLY_SOURCE_WAS_LOST_OR_FABRICATED';
  END IF;
  PERFORM pg_temp.bank_source_count('PAYE',0,0);
  -- The unchanged filter retains a bank source with no published client. Do
  -- not invent a client ID or silently change that established behaviour.
  PERFORM pg_temp.bank_source_count('ALL',1,1,'{"client_id":"10000000-0000-4000-8000-000000009993"}');
  UPDATE public.banking_pay_workbench_session_candidate_state
  SET effective_payees_json=jsonb_build_array(effective_payees_json->0 || '{"client_id":"10000000-0000-4000-8000-000000009992"}'),
      effective_non_paye_payee_json=effective_non_paye_payee_json || '{"client_id":"10000000-0000-4000-8000-000000009992"}'
  WHERE session_id=v_id AND candidate_id='10000000-0000-4000-8000-000000006001';
  PERFORM pg_temp.bank_source_count('ALL',0,0,'{"client_id":"10000000-0000-4000-8000-000000009993"}');
  PERFORM pg_temp.bank_source_count('ALL',105,105,'{"client_id":"10000000-0000-4000-8000-000000009992"}');
  PERFORM pg_temp.bank_source_count('ALL',1,1,'{"candidate_id":"10000000-0000-4000-8000-000000006001"}');
  UPDATE public.banking_pay_workbench_session_candidate_state
  SET effective_candidate_fragment_json=effective_candidate_fragment_json || '{"presentation_role":"HIDDEN_INDEFINITE_SNOOZE"}'
  WHERE session_id=v_id AND candidate_id='10000000-0000-4000-8000-000000006001';
  PERFORM pg_temp.bank_source_count('ALL',104,104);
  UPDATE public.banking_pay_workbench_session_candidate_state
  SET effective_candidate_fragment_json=effective_candidate_fragment_json-'presentation_role'
  WHERE session_id=v_id AND candidate_id='10000000-0000-4000-8000-000000006001';
  INSERT INTO public.banking_pay_workbench_preview_rows SELECT (v_saved).*;
  PERFORM pg_temp.bank_source_count('ALL',105,105);
  -- A corrupted fragment may not silently be reassigned to a different owner.
  UPDATE public.banking_pay_workbench_session_candidate_state SET effective_payees_json=jsonb_build_array(
    effective_payees_json->0 || '{"candidate_id":"10000000-0000-4000-8000-000000009994"}')
  WHERE session_id=v_id AND candidate_id='10000000-0000-4000-8000-000000000003';
  BEGIN
    PERFORM * FROM private.pay_workbench_modal_bank_sources_v2(v_session,'ALL');
    RAISE EXCEPTION 'CORRUPT_BANK_SOURCE_OWNER_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN
    GET STACKED DIAGNOSTICS v_error=MESSAGE_TEXT;
    IF v_error<>'BANKING_PAY_V2_SOURCE_IDENTITY_MISMATCH' THEN RAISE; END IF;
  END;
  SET LOCAL client_min_messages='notice';
  RAISE NOTICE 'PASS: 105 candidates share one bank owner without lost candidates or duplicate aliases; physical precedence, filters, hidden/version/pending and corrupt-owner fences.';
END;
$sources$;
ROLLBACK;
