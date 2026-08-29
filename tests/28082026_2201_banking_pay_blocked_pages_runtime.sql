\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='30s';
SET LOCAL client_min_messages='warning';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
UPDATE public.banking_pay_workbench_sessions SET scope_seed_complete=true WHERE id='10000000-0000-4000-8000-000000000005';
UPDATE public.candidates SET display_name='Literal %_ Blocked fixture' WHERE id='10000000-0000-4000-8000-000000000002';
UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json||'{"presentation_role":"HIDDEN_INDEFINITE_SNOOZE"}'::jsonb
 WHERE session_id='10000000-0000-4000-8000-000000000005' AND row_ordinal>200;
UPDATE public.banking_pay_workbench_preview_rows SET section='blocked_for_pay',selected=false,selection_state='NOT_SELECTABLE',
 row_json=row_json||jsonb_build_object('presentation_section','BLOCKED_FOR_PAY','readiness_state','BLOCKED_FOR_PAY',
 'selection_allowed',false,'draftable',false,'is_ready_for_draft',false,
 'amount_display',(row_ordinal-50)::text,'section_amount_display',(row_ordinal-50)::text,
 'line_type',CASE row_ordinal%3 WHEN 0 THEN 'DO_NOT_PAY' WHEN 1 THEN 'BLOCKED_TIMESHEET' ELSE 'TIMESHEET_PAYMENT' END)
 WHERE session_id='10000000-0000-4000-8000-000000000005' AND row_ordinal<=109;
UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json||'{"snooze_state":{"state":"SNOOZED","snooze_until_date":"2026-09-01"}}'::jsonb
 WHERE session_id='10000000-0000-4000-8000-000000000005' AND row_ordinal=1;
CREATE TEMP TABLE blocked_page_results(label text PRIMARY KEY,payload jsonb) ON COMMIT DROP;
CREATE TEMP TABLE blocked_page_summary(payload jsonb) ON COMMIT DROP;
CREATE TEMP TABLE blocked_page_original(hash text) ON COMMIT DROP;
INSERT INTO blocked_page_original SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text)
 FROM public.banking_pay_workbench_preview_rows r WHERE r.session_id='10000000-0000-4000-8000-000000000005';
CREATE FUNCTION pg_temp.read_blocked(p_sort text DEFAULT 'CANDIDATE',p_direction text DEFAULT 'ASC',
 p_cursor text DEFAULT NULL,p_limit integer DEFAULT 100,p_search text DEFAULT '') RETURNS jsonb LANGUAGE plpgsql AS $read$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;
BEGIN
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
  'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
 RETURN public.pay_workbench_session_get_blocked_page_v1(s.id,opts,s.actor_user_id,p_sort,p_direction,p_cursor,p_limit,p_search);
END;
$read$;
INSERT INTO blocked_page_summary SELECT public.pay_workbench_session_get_candidate_summary_page_v1(s.id,
 jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
 'pay_channel_scope','ALL'),s.actor_user_id) FROM public.banking_pay_workbench_sessions s WHERE id='10000000-0000-4000-8000-000000000005';
INSERT INTO blocked_page_results VALUES('CANDIDATE_ASC',pg_temp.read_blocked());
INSERT INTO blocked_page_results SELECT 'CANDIDATE_ASC_next',pg_temp.read_blocked(p_cursor=>payload->>'next_cursor') FROM blocked_page_results WHERE label='CANDIDATE_ASC';
INSERT INTO blocked_page_results VALUES('CANDIDATE_DESC',pg_temp.read_blocked('CANDIDATE','DESC'));
INSERT INTO blocked_page_results SELECT 'CANDIDATE_DESC_next',pg_temp.read_blocked('CANDIDATE','DESC',payload->>'next_cursor') FROM blocked_page_results WHERE label='CANDIDATE_DESC';
INSERT INTO blocked_page_results VALUES('REASON_ASC',pg_temp.read_blocked('REASON','ASC'));
INSERT INTO blocked_page_results SELECT 'REASON_ASC_next',pg_temp.read_blocked('REASON','ASC',payload->>'next_cursor') FROM blocked_page_results WHERE label='REASON_ASC';
INSERT INTO blocked_page_results VALUES('REASON_DESC',pg_temp.read_blocked('REASON','DESC'));
INSERT INTO blocked_page_results SELECT 'REASON_DESC_next',pg_temp.read_blocked('REASON','DESC',payload->>'next_cursor') FROM blocked_page_results WHERE label='REASON_DESC';
INSERT INTO blocked_page_results VALUES('AMOUNT_ASC',pg_temp.read_blocked('AMOUNT','ASC'));
INSERT INTO blocked_page_results SELECT 'AMOUNT_ASC_next',pg_temp.read_blocked('AMOUNT','ASC',payload->>'next_cursor') FROM blocked_page_results WHERE label='AMOUNT_ASC';
INSERT INTO blocked_page_results VALUES('AMOUNT_DESC',pg_temp.read_blocked('AMOUNT','DESC'));
INSERT INTO blocked_page_results SELECT 'AMOUNT_DESC_next',pg_temp.read_blocked('AMOUNT','DESC',payload->>'next_cursor') FROM blocked_page_results WHERE label='AMOUNT_DESC';
INSERT INTO blocked_page_results VALUES('forty_first',pg_temp.read_blocked(p_limit=>40));
INSERT INTO blocked_page_results SELECT 'forty_second',pg_temp.read_blocked(p_cursor=>payload->>'next_cursor',p_limit=>40) FROM blocked_page_results WHERE label='forty_first';
INSERT INTO blocked_page_results SELECT 'forty_third',pg_temp.read_blocked(p_cursor=>payload->>'next_cursor',p_limit=>40) FROM blocked_page_results WHERE label='forty_second';
INSERT INTO blocked_page_results SELECT 'forty_previous',pg_temp.read_blocked(p_cursor=>payload->>'previous_cursor',p_limit=>40) FROM blocked_page_results WHERE label='forty_third';
INSERT INTO blocked_page_results VALUES('literal',pg_temp.read_blocked(p_search=>'%_'));
INSERT INTO blocked_page_results VALUES('empty',pg_temp.read_blocked(p_search=>'no blocked fixture matches'));
DO $proof$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;a jsonb;b jsonb;bad jsonb;k text;d jsonb;current_hash text;
BEGIN
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
  'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
 SELECT payload INTO STRICT a FROM blocked_page_results WHERE label='CANDIDATE_ASC';
 SELECT payload INTO STRICT b FROM blocked_page_results WHERE label='CANDIDATE_ASC_next';
 IF a->>'total_count'<>'109' OR jsonb_array_length(a->'rows')<>100 OR jsonb_array_length(b->'rows')<>9
  OR a->>'page_number'<>'1' OR b->>'page_number'<>'2' OR b->'has_more'<>'false'::jsonb THEN RAISE EXCEPTION 'BLOCKED_LOST_PAGE';END IF;
 IF (SELECT payload FROM blocked_page_results WHERE label='forty_second') IS DISTINCT FROM
  (SELECT payload FROM blocked_page_results WHERE label='forty_previous') THEN RAISE EXCEPTION 'BLOCKED_PREVIOUS_CHANGED_PAGE';END IF;
 IF (SELECT payload->>'total_count' FROM blocked_page_results WHERE label='literal')<>'107'
  OR (SELECT payload->>'total_count' FROM blocked_page_results WHERE label='empty')<>'0' THEN RAISE EXCEPTION 'BLOCKED_SEARCH_NOT_LITERAL';END IF;
 IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(a->'rows') r WHERE r->>'reason'='Snoozed until 01/09/2026.') THEN
  RAISE EXCEPTION 'BLOCKED_DATED_SNOOZE_LOST';END IF;
 SELECT r->>'identity' INTO k FROM jsonb_array_elements(a->'rows') r LIMIT 1;
 d:=public.pay_workbench_session_get_blocked_detail_v1(s.id,opts,s.actor_user_id,k);
 IF jsonb_array_length(d->'rows')<>1 OR d#>>'{rows,0,source_kind}'<>'PREVIEW_ROW' THEN RAISE EXCEPTION 'BLOCKED_DETAIL_WRONG_OWNER';END IF;
 IF d#>'{rows,0,payload}' IS DISTINCT FROM (SELECT private.pay_workbench_modal_row_payload_v2(r)
   FROM public.banking_pay_workbench_preview_rows r WHERE r.id=(d#>>'{rows,0,preview_row_id}')::uuid) THEN
  RAISE EXCEPTION 'BLOCKED_DETAIL_CHANGED_PAYLOAD';END IF;
 bad:=private.pay_workbench_modal_cursor_decode_v2(a->>'next_cursor','{}');
 BEGIN
  PERFORM pg_temp.read_blocked(p_cursor=>a->>'next_cursor',p_search=>'different');
  RAISE EXCEPTION 'BLOCKED_CHANGED_SEARCH_ACCEPTED';
 EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE;END IF;END;
 BEGIN
  PERFORM pg_temp.read_blocked(p_cursor=>private.pay_workbench_modal_cursor_encode_v2(bad||jsonb_build_object('last_identity',repeat('0',64))));
  RAISE EXCEPTION 'BLOCKED_MISSING_BOUNDARY_ACCEPTED';
 EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE;END IF;END;
 BEGIN
  PERFORM public.pay_workbench_session_get_blocked_page_v1(s.id,opts,'10000000-0000-4000-8000-000000009999');
  RAISE EXCEPTION 'BLOCKED_WRONG_ACTOR_ACCEPTED';
 EXCEPTION WHEN insufficient_privilege THEN IF SQLERRM<>'BANKING_PAY_V2_UNAUTHORISED' THEN RAISE;END IF;END;
 BEGIN
  PERFORM public.pay_workbench_session_get_blocked_page_v1(s.id,opts||'{"expected_progress_counter_version":999}'::jsonb,s.actor_user_id);
  RAISE EXCEPTION 'BLOCKED_STALE_REVISION_ACCEPTED';
 EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_REVISION' THEN RAISE;END IF;END;
 BEGIN
  PERFORM pg_temp.read_blocked(p_sort=>'AMOUNT',p_cursor=>a->>'next_cursor');
  RAISE EXCEPTION 'BLOCKED_CHANGED_SORT_ACCEPTED';
 EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE;END IF;END;
 FOREACH k IN ARRAY ARRAY[repeat('x',201),repeat(chr(128512),101),chr(10),' leading'] LOOP
  BEGIN
   PERFORM pg_temp.read_blocked(p_search=>k);RAISE EXCEPTION 'BLOCKED_INVALID_SEARCH_ACCEPTED';
  EXCEPTION WHEN invalid_parameter_value THEN IF SQLERRM<>'BANKING_PAY_V2_INVALID_INPUT' THEN RAISE;END IF;END;
 END LOOP;
 SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text) INTO current_hash FROM public.banking_pay_workbench_preview_rows r WHERE r.session_id=s.id;
 IF current_hash IS DISTINCT FROM (SELECT hash FROM blocked_page_original) THEN RAISE EXCEPTION 'BLOCKED_READ_CHANGED_PAYMENT';END IF;
END;
$proof$;
SAVEPOINT source_only;
UPDATE public.banking_pay_workbench_session_scope SET status='FAILED' WHERE session_id='10000000-0000-4000-8000-000000000005'
 AND candidate_id='10000000-0000-4000-8000-000000000003';
DO $source$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;r record;n integer:=0;
BEGIN
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 FOR r IN SELECT * FROM private.pay_workbench_modal_blocked_summaries_v2(s,'ALL','CSV','PROD','{}',true,false)
  WHERE candidate_id='10000000-0000-4000-8000-000000000003' LOOP
  n:=n+1;IF r.source_kind<>'SOURCE_PROGRESS' OR r.preview_row_id IS NOT NULL OR r.affected_display_amount IS NOT NULL
   OR r.reason<>'Refresh failed' THEN RAISE EXCEPTION 'BLOCKED_SOURCE_INVENTED_PAYMENT';END IF;
 END LOOP;
 IF n<>1 THEN RAISE EXCEPTION 'BLOCKED_SOURCE_LOST_OR_DUPLICATED';END IF;
END;
$source$;
ROLLBACK TO SAVEPOINT source_only;
SAVEPOINT stored_bank;
UPDATE public.settings_defaults SET rail_provider_default='REVOLUT',rail_env_default='SANDBOX' WHERE id=1;
INSERT INTO public.umbrellas(id,name,enabled,sort_code,account_number)
 VALUES('10000000-0000-4000-8000-000000009991','Disposable blocked bank fixture',true,'00-00-00','00000000');
INSERT INTO public.candidates(id,display_name,umbrella_id,pay_method)
 SELECT ('10000000-0000-4000-8000-'||lpad((20000+n)::text,12,'0'))::uuid,'Stored bank blocker '||n,
 '10000000-0000-4000-8000-000000009991'::uuid,'UMBRELLA' FROM generate_series(1,3) n;
INSERT INTO public.banking_pay_workbench_session_scope(session_id,candidate_id,scope_ordinal,status,seeded,dirty)
 SELECT '10000000-0000-4000-8000-000000000005'::uuid,('10000000-0000-4000-8000-'||lpad((20000+n)::text,12,'0'))::uuid,
 100+n,'READY',true,false FROM generate_series(1,3) n;
INSERT INTO public.banking_pay_workbench_session_candidate_state(session_id,candidate_id,session_version,status,
 effective_candidate_fragment_json,effective_payees_json)
 SELECT '10000000-0000-4000-8000-000000000005'::uuid,c.id,1,'READY',
 jsonb_build_object('candidate_id',c.id,'current_pay_method','UMBRELLA'),
 jsonb_build_array(jsonb_build_object('candidate_id',c.id,'payee_entity_kind','UMBRELLA','payee_entity_id',u.id,
 'bank_details_hash',u.bank_details_hash,'pay_channel','UMBRELLA','blockers',jsonb_build_array('BLOCKED_NAME_CHECK'),'name_check_status','NEAR_MATCH'))
 FROM public.candidates c JOIN public.umbrellas u ON u.id=c.umbrella_id WHERE u.id='10000000-0000-4000-8000-000000009991';
INSERT INTO public.bank_name_checks(rail_provider,rail_env,entity_kind,entity_id,bank_details_hash,status,checked_at_utc)
 SELECT 'REVOLUT','SANDBOX','UMBRELLA',id,bank_details_hash,'PASS',now() FROM public.umbrellas WHERE id='10000000-0000-4000-8000-000000009991';
CREATE TEMP TABLE blocked_null_results(label text,payload jsonb) ON COMMIT DROP;
INSERT INTO blocked_null_results VALUES('ASC',pg_temp.read_blocked('AMOUNT','ASC'));
INSERT INTO blocked_null_results SELECT 'ASC_next',pg_temp.read_blocked('AMOUNT','ASC',payload->>'next_cursor') FROM blocked_null_results WHERE label='ASC';
INSERT INTO blocked_null_results VALUES('DESC',pg_temp.read_blocked('AMOUNT','DESC'));
INSERT INTO blocked_null_results SELECT 'DESC_next',pg_temp.read_blocked('AMOUNT','DESC',payload->>'next_cursor') FROM blocked_null_results WHERE label='DESC';
DO $bank$
DECLARE p record;s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;r jsonb;d jsonb;channel text;
BEGIN
 FOR p IN SELECT * FROM blocked_null_results LOOP
  IF p.payload->>'total_count'<>'112' THEN RAISE EXCEPTION 'BLOCKED_BANK_LOST_SOURCE: %',p.payload->>'total_count';END IF;
  IF p.label LIKE '%next' THEN
   IF jsonb_array_length(p.payload->'rows')<>12
    OR (SELECT count(*) FROM jsonb_array_elements(p.payload->'rows') WITH ORDINALITY m(value,n)
      WHERE n>9 AND value->'affected_display_amount'='null'::jsonb AND value->>'source_kind'='STORED_PAYEE'
       AND value->'preview_row_id'='null'::jsonb AND value->>'reason_message_id'='MSG-096')<>3 THEN
    RAISE EXCEPTION 'BLOCKED_NULL_AMOUNT_ORDER_OR_MEMBERSHIP';END IF;
  ELSIF EXISTS(SELECT 1 FROM jsonb_array_elements(p.payload->'rows') m WHERE m->'affected_display_amount'='null'::jsonb) THEN
   RAISE EXCEPTION 'BLOCKED_NULL_AMOUNT_SORTED_FIRST';END IF;
 END LOOP;
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
  'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
 SELECT payload#>'{rows,11}' INTO r FROM blocked_null_results WHERE label='ASC_next';
 d:=public.pay_workbench_session_get_blocked_detail_v1(s.id,opts,s.actor_user_id,r->>'identity');
 IF d->>'total_count'<>'1' OR d#>>'{rows,0,source_kind}'<>'STORED_PAYEE'
  OR d#>'{rows,0,preview_row_id}'<>'null'::jsonb OR d#>>'{rows,0,task_meta,code}'<>'BANK_RESULT_CHANGED' THEN
  RAISE EXCEPTION 'BLOCKED_BANK_DETAIL_LOST_OR_INVENTED_PAYMENT';END IF;
 FOREACH channel IN ARRAY ARRAY['PAYE','UMBRELLA'] LOOP
  opts:=opts||jsonb_build_object('pay_channel_scope',channel,'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,channel));
  d:=public.pay_workbench_session_get_blocked_page_v1(s.id,opts,s.actor_user_id);
  IF (d->>'total_count')::integer<>(CASE channel WHEN 'PAYE' THEN 105 ELSE 7 END) THEN
   RAISE EXCEPTION 'BLOCKED_CHANNEL_FILTER_LOST_OR_WIDENED';END IF;
 END LOOP;
END;
$bank$;
WITH summary AS MATERIALIZED (
 SELECT public.pay_workbench_session_get_candidate_summary_page_v1(s.id,jsonb_build_object(
  'expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,'pay_channel_scope','ALL'),
  s.actor_user_id) AS payload FROM public.banking_pay_workbench_sessions s WHERE id='10000000-0000-4000-8000-000000000005'
)
SELECT jsonb_build_object('label','bank_'||r.label,'payload',r.payload,'summary',s.payload)
 FROM blocked_null_results r CROSS JOIN summary s;
UPDATE public.bank_name_checks SET status='NEAR_MATCH' WHERE entity_id='10000000-0000-4000-8000-000000009991';
DO $action_exclusion$
DECLARE p jsonb;
BEGIN
 p:=pg_temp.read_blocked();
 IF p->>'total_count'<>'109' THEN RAISE EXCEPTION 'BLOCKED_RETAINED_ACTIONABLE_BANK_TASK';END IF;
END;
$action_exclusion$;
ROLLBACK TO SAVEPOINT stored_bank;
SELECT jsonb_build_object('label',r.label,'payload',r.payload,'summary',s.payload) FROM blocked_page_results r CROSS JOIN blocked_page_summary s;
SET LOCAL client_min_messages='notice';
DO $done$ BEGIN RAISE NOTICE 'PASS: complete Blocked list109 items; six sorts; exact Previous/search/detail; source-only retained; no read writes.';END $done$;
ROLLBACK;
