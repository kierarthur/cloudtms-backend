\set ON_ERROR_STOP on
BEGIN;
SET LOCAL statement_timeout='30s';
SET LOCAL client_min_messages='warning';
\ir fixtures/28082026_1429_banking_pay_selection_setup.sql
CREATE TEMP TABLE task_page_results(label text PRIMARY KEY,payload jsonb) ON COMMIT DROP;
CREATE TEMP TABLE task_page_original(hash text) ON COMMIT DROP;
CREATE TEMP TABLE task_page_summaries(label text PRIMARY KEY,payload jsonb) ON COMMIT DROP;
UPDATE public.banking_pay_workbench_sessions SET scope_seed_complete=true WHERE id='10000000-0000-4000-8000-000000000005';
UPDATE public.candidates SET display_name='Literal %_ Task fixture' WHERE id='10000000-0000-4000-8000-000000000002';
UPDATE public.banking_pay_workbench_preview_rows SET section='cases_resolutions',selected=false,selection_state='NOT_SELECTABLE',
 row_json=row_json||jsonb_build_object('presentation_section','CASES_RESOLUTIONS','readiness_state','CASES_RESOLUTIONS',
 'case_key','finance:10000000-0000-4000-8000-'||lpad((30000+row_ordinal)::text,12,'0'),
 'finance_case_id','10000000-0000-4000-8000-'||lpad((30000+row_ordinal)::text,12,'0'),
 'resolution_family','NON_BUCKET','case_needs_resolution',true,'case_resolution_satisfied_now',false)
WHERE session_id='10000000-0000-4000-8000-000000000005' AND row_ordinal<=105;
INSERT INTO task_page_original SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text)
 FROM public.banking_pay_workbench_preview_rows r WHERE r.session_id='10000000-0000-4000-8000-000000000005';
CREATE FUNCTION pg_temp.read_tasks(p_sort text DEFAULT 'TITLE',p_direction text DEFAULT 'ASC',p_cursor text DEFAULT NULL,
 p_limit integer DEFAULT 100,p_search text DEFAULT '',p_view text DEFAULT 'ACTION_REQUIRED') RETURNS jsonb LANGUAGE plpgsql AS $read$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;
BEGIN
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
  'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
 RETURN public.pay_workbench_session_get_action_required_page_v1(s.id,opts,s.actor_user_id,p_sort,p_direction,p_cursor,p_limit,p_search,p_view);
END;
$read$;
CREATE FUNCTION pg_temp.read_task_summary() RETURNS jsonb LANGUAGE plpgsql AS $read$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;
BEGIN
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 RETURN public.pay_workbench_session_get_candidate_summary_page_v1(s.id,jsonb_build_object(
  'expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,'pay_channel_scope','ALL'),s.actor_user_id);
END;
$read$;
INSERT INTO task_page_summaries VALUES('initial',pg_temp.read_task_summary());
-- Separate top-level reads retain the existing PREVIEW_PROGRESS per-call budget.
INSERT INTO task_page_results VALUES('first',pg_temp.read_tasks());
INSERT INTO task_page_results SELECT 'next',pg_temp.read_tasks(p_cursor=>payload->>'next_cursor') FROM task_page_results WHERE label='first';
INSERT INTO task_page_results VALUES('forty_first',pg_temp.read_tasks(p_limit=>40));
INSERT INTO task_page_results SELECT 'forty_second',pg_temp.read_tasks(p_cursor=>payload->>'next_cursor',p_limit=>40) FROM task_page_results WHERE label='forty_first';
INSERT INTO task_page_results SELECT 'forty_third',pg_temp.read_tasks(p_cursor=>payload->>'next_cursor',p_limit=>40) FROM task_page_results WHERE label='forty_second';
INSERT INTO task_page_results SELECT 'forty_previous',pg_temp.read_tasks(p_cursor=>payload->>'previous_cursor',p_limit=>40) FROM task_page_results WHERE label='forty_third';
INSERT INTO task_page_results VALUES('TITLE_ASC',pg_temp.read_tasks('TITLE','ASC'));
INSERT INTO task_page_results VALUES('TITLE_DESC',pg_temp.read_tasks('TITLE','DESC'));
INSERT INTO task_page_results VALUES('CANDIDATES_ASC',pg_temp.read_tasks('CANDIDATES','ASC'));
INSERT INTO task_page_results VALUES('CANDIDATES_DESC',pg_temp.read_tasks('CANDIDATES','DESC'));
INSERT INTO task_page_results VALUES('PAYMENTS_ASC',pg_temp.read_tasks('PAYMENTS','ASC'));
INSERT INTO task_page_results VALUES('PAYMENTS_DESC',pg_temp.read_tasks('PAYMENTS','DESC'));
INSERT INTO task_page_results VALUES('AMOUNT_ASC',pg_temp.read_tasks('AMOUNT','ASC'));
INSERT INTO task_page_results VALUES('AMOUNT_DESC',pg_temp.read_tasks('AMOUNT','DESC'));
INSERT INTO task_page_results VALUES('literal_search',pg_temp.read_tasks(p_search=>'%_'));
INSERT INTO task_page_results VALUES('empty_search',pg_temp.read_tasks(p_search=>'no matching task fixture'));
DO $pages$
DECLARE a jsonb;b jsonb;c jsonb;t record;v_bad text;opts jsonb;s public.banking_pay_workbench_sessions%ROWTYPE;current_hash text;decoded jsonb;
BEGIN
 SELECT payload INTO STRICT a FROM task_page_results WHERE label='first';
 SELECT payload INTO STRICT b FROM task_page_results WHERE label='next';
 IF a->>'view'<>'ACTION_REQUIRED' OR a->>'total_count'<>'105' OR a->>'scope_count'<>'105' OR jsonb_array_length(a->'rows')<>100
  OR a->>'page_number'<>'1' OR a->'has_previous'<>'false'::jsonb OR a->'has_more'<>'true'::jsonb
  OR a->'previous_cursor'<>'null'::jsonb OR a->>'updating_count'<>'0' OR a->'updating'<>'[]'::jsonb
  OR a->'updating_has_more'<>'false'::jsonb OR a->'updating_next_cursor'<>'null'::jsonb THEN RAISE EXCEPTION 'ACTION_FIRST_PAGE_INCOMPLETE';END IF;
 IF b->>'total_count'<>'105' OR b->>'page_number'<>'2' OR jsonb_array_length(b->'rows')<>5
  OR b->'has_previous'<>'true'::jsonb OR b->'has_more'<>'false'::jsonb OR b->'previous_cursor'<>'null'::jsonb THEN
  RAISE EXCEPTION 'ACTION_SECOND_PAGE_INCOMPLETE';END IF;
 IF (SELECT count(DISTINCT r->>'identity') FROM jsonb_array_elements((a->'rows')||(b->'rows')) r)<>105
  OR EXISTS(SELECT 1 FROM jsonb_array_elements((a->'rows')||(b->'rows')) r WHERE r->>'affected_candidate_count'<>'1'
   OR r->>'affected_payment_count'<>'1' OR r->'affected_payment_count_complete'<>'true'::jsonb
   OR r->>'title'<>'Amount decision required' OR r ? 'search_text') THEN RAISE EXCEPTION 'ACTION_TASKS_DUPLICATED_OR_COUNTS_CHANGED';END IF;
 SELECT payload INTO STRICT c FROM task_page_results WHERE label='forty_third';
 IF jsonb_array_length(c->'rows')<>25 OR c->>'page_number'<>'3' OR c->'has_previous'<>'true'::jsonb THEN RAISE EXCEPTION 'ACTION_THIRD_PAGE';END IF;
 IF (SELECT payload FROM task_page_results WHERE label='forty_previous') IS DISTINCT FROM
  (SELECT payload FROM task_page_results WHERE label='forty_second') THEN RAISE EXCEPTION 'ACTION_PREVIOUS_CHANGED_PAGE';END IF;
 FOR t IN SELECT * FROM task_page_results WHERE label IN ('TITLE_ASC','TITLE_DESC','CANDIDATES_ASC','CANDIDATES_DESC','PAYMENTS_ASC','PAYMENTS_DESC','AMOUNT_ASC','AMOUNT_DESC') LOOP
  IF t.payload->>'total_count'<>'105' OR jsonb_array_length(t.payload->'rows')<>100
   OR (SELECT count(DISTINCT r->>'identity') FROM jsonb_array_elements(t.payload->'rows') r)<>100 THEN
   RAISE EXCEPTION 'ACTION_SORT_CHANGED_SCOPE: %',t.label;END IF;
 END LOOP;
 SELECT payload INTO STRICT c FROM task_page_results WHERE label='literal_search';
 IF c->'rows' IS DISTINCT FROM a->'rows' THEN RAISE EXCEPTION 'ACTION_LITERAL_SEARCH_NOT_LITERAL';END IF;
 SELECT payload INTO STRICT c FROM task_page_results WHERE label='empty_search';
 IF c->>'scope_count'<>'105' OR c->>'total_count'<>'0' OR c->>'page_number'<>'0' OR c->'rows'<>'[]'::jsonb
  OR c->'has_previous'<>'false'::jsonb OR c->'has_more'<>'false'::jsonb THEN RAISE EXCEPTION 'ACTION_EMPTY_SEARCH_LOST_SCOPE';END IF;
 FOREACH v_bad IN ARRAY ARRAY[repeat('x',201),repeat(chr(128512),101),' leading','trailing ',chr(10),chr(127)] LOOP
  BEGIN PERFORM pg_temp.read_tasks(p_search=>v_bad);RAISE EXCEPTION 'ACTION_BAD_SEARCH_ACCEPTED';
   EXCEPTION WHEN invalid_parameter_value THEN IF SQLERRM<>'BANKING_PAY_V2_INVALID_INPUT' THEN RAISE;END IF;END;
 END LOOP;
 BEGIN PERFORM pg_temp.read_tasks(p_sort=>'GROSS');RAISE EXCEPTION 'ACTION_BAD_SORT_ACCEPTED';
  EXCEPTION WHEN invalid_parameter_value THEN IF SQLERRM<>'BANKING_PAY_V2_INVALID_INPUT' THEN RAISE;END IF;END;
 BEGIN PERFORM pg_temp.read_tasks(p_view=>'BLOCKED');RAISE EXCEPTION 'ACTION_BAD_VIEW_ACCEPTED';
  EXCEPTION WHEN invalid_parameter_value THEN IF SQLERRM<>'BANKING_PAY_V2_INVALID_INPUT' THEN RAISE;END IF;END;
 BEGIN PERFORM pg_temp.read_tasks(p_view=>'UPDATING',p_search=>'anything');RAISE EXCEPTION 'UPDATING_SEARCH_ACCEPTED';
  EXCEPTION WHEN invalid_parameter_value THEN IF SQLERRM<>'BANKING_PAY_V2_INVALID_INPUT' THEN RAISE;END IF;END;
 BEGIN PERFORM pg_temp.read_tasks(p_cursor=>a->>'next_cursor',p_search=>'%_');RAISE EXCEPTION 'ACTION_CROSS_SEARCH_CURSOR_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE;END IF;END;
 BEGIN PERFORM pg_temp.read_tasks(p_cursor=>a->>'next_cursor',p_view=>'UPDATING');RAISE EXCEPTION 'ACTION_CROSS_VIEW_CURSOR_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE;END IF;END;
 decoded:=private.pay_workbench_modal_cursor_decode_v2(a->>'next_cursor','{}');
 FOREACH v_bad IN ARRAY ARRAY[repeat('0',64),a#>>'{rows,0,identity}'] LOOP
  BEGIN PERFORM pg_temp.read_tasks(p_cursor=>private.pay_workbench_modal_cursor_encode_v2(decoded||jsonb_build_object('last_identity',v_bad)));
   RAISE EXCEPTION 'ACTION_MISSING_OR_UNALIGNED_BOUNDARY_ACCEPTED';
   EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE;END IF;END;
 END LOOP;
 BEGIN PERFORM pg_temp.read_tasks(p_cursor=>private.pay_workbench_modal_cursor_encode_v2(decoded||'{"last_identity":null}'));
  RAISE EXCEPTION 'ACTION_MALFORMED_BOUNDARY_ACCEPTED';
  EXCEPTION WHEN invalid_parameter_value THEN IF SQLERRM<>'BANKING_PAY_V2_INVALID_CURSOR' THEN RAISE;END IF;END;
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
  'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
 BEGIN PERFORM public.pay_workbench_session_get_action_required_page_v1(s.id,opts,'10000000-0000-4000-8000-000000009999');
  RAISE EXCEPTION 'ACTION_UNAUTHORISED_ACCEPTED';
  EXCEPTION WHEN insufficient_privilege THEN IF SQLERRM<>'BANKING_PAY_V2_UNAUTHORISED' THEN RAISE;END IF;END;
 BEGIN PERFORM public.pay_workbench_session_get_action_required_page_v1(s.id,opts||'{"expected_progress_counter_version":3}',s.actor_user_id);
  RAISE EXCEPTION 'ACTION_STALE_REVISION_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_REVISION' THEN RAISE;END IF;END;
 SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text) INTO current_hash FROM public.banking_pay_workbench_preview_rows r WHERE r.session_id=s.id;
 IF current_hash IS DISTINCT FROM (SELECT hash FROM task_page_original) THEN RAISE EXCEPTION 'ACTION_READ_CHANGED_PAYMENTS';END IF;
END;
$pages$;
SAVEPOINT varying_task_sorts;
UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json||jsonb_build_object('resolution_family',
 CASE WHEN row_ordinal%3=0 THEN 'BUCKETED' WHEN row_ordinal%3=1 THEN 'TAXABLE_CHANNEL_RESTRUCTURE' ELSE 'NON_BUCKET' END,
 'amount_display',(10+row_ordinal)::numeric(16,2)::text)
WHERE session_id='10000000-0000-4000-8000-000000000005' AND row_ordinal<=105;
INSERT INTO public.banking_pay_workbench_preview_rows(id,session_id,candidate_id,section,row_key,row_ordinal,row_json,key_type,key_value,selected,selection_state,status,session_version)
SELECT ('10000000-0000-4000-8000-'||lpad((5000+n)::text,12,'0'))::uuid,r.session_id,r.candidate_id,r.section,
 'extra-task-member:'||n,500+n,r.row_json,'SOURCE_REF','extra-task-member:'||n,false,'NOT_SELECTABLE','READY',1
FROM generate_series(1,3) n JOIN public.banking_pay_workbench_preview_rows r
 ON r.session_id='10000000-0000-4000-8000-000000000005' AND r.row_ordinal=CASE WHEN n<=2 THEN 1 ELSE 2 END;
INSERT INTO public.candidates(id,display_name,tms_ref,pay_method)
SELECT ('10000000-0000-4000-8000-'||lpad((8000+n)::text,12,'0'))::uuid,'Unconfirmed sort fixture '||n,'TASK-SORT-'||n,'PAYE'
FROM generate_series(1,2) n;
INSERT INTO public.banking_pay_workbench_session_scope(session_id,candidate_id,scope_ordinal,status,seeded,dirty)
SELECT '10000000-0000-4000-8000-000000000005'::uuid,('10000000-0000-4000-8000-'||lpad((8000+n)::text,12,'0'))::uuid,
 n+2,'FAILED',true,false FROM generate_series(1,2) n;
CREATE FUNCTION pg_temp.verify_varied_task_sort(p_sort text,p_direction text) RETURNS void LANGUAGE plpgsql AS $sort$
DECLARE a jsonb;b jsonb;all_rows jsonb;bad_count bigint;variation bigint;
BEGIN
 a:=pg_temp.read_tasks(p_sort,p_direction);
 b:=pg_temp.read_tasks(p_sort,p_direction,a->>'next_cursor');all_rows:=(a->'rows')||(b->'rows');
 IF a->>'total_count'<>'106' OR jsonb_array_length(all_rows)<>106
  OR (SELECT count(DISTINCT x->>'identity') FROM jsonb_array_elements(all_rows) x)<>106 THEN RAISE EXCEPTION 'VARIED_TASK_SORT_LOST_MEMBERS';END IF;
 WITH values AS (SELECT n,r->>'identity' AS id,
  CASE p_sort WHEN 'CANDIDATES' THEN CASE WHEN r->>'candidate_name' IS NOT NULL
      THEN ('0|'||lower(r->>'candidate_name')||'|'||lower(COALESCE(r->>'candidate_reference',''))) COLLATE "C"
      ELSE '1|'||lpad((r->>'affected_candidate_count')::text,20,'0') END
    ELSE lower(r->>'title') COLLATE "C" END AS title,
  CASE p_sort WHEN 'PAYMENTS' THEN (r->>'affected_payment_count')::numeric
    WHEN 'AMOUNT' THEN (r->>'affected_display_amount')::numeric END AS amount
  FROM jsonb_array_elements(all_rows) WITH ORDINALITY e(r,n)), compared AS (
  SELECT v.*,lag(id) OVER(ORDER BY n) AS old_id,lag(title) OVER(ORDER BY n) AS old_title,
   lag(amount) OVER(ORDER BY n) AS old_amount FROM values v)
 SELECT count(*) FILTER(WHERE n>1 AND CASE WHEN p_sort IN ('TITLE','CANDIDATES') THEN
    (title IS NOT NULL AND old_title IS NULL)
     OR CASE WHEN p_direction='ASC' THEN title<old_title ELSE title>old_title END
     OR (title IS NOT DISTINCT FROM old_title AND CASE WHEN p_direction='ASC' THEN id COLLATE "C"<old_id COLLATE "C" ELSE id COLLATE "C">old_id COLLATE "C" END)
   ELSE (amount IS NOT NULL AND old_amount IS NULL)
     OR CASE WHEN p_direction='ASC' THEN amount<old_amount ELSE amount>old_amount END
     OR (amount IS NOT DISTINCT FROM old_amount AND CASE WHEN p_direction='ASC' THEN id COLLATE "C"<old_id COLLATE "C" ELSE id COLLATE "C">old_id COLLATE "C" END) END),
  CASE WHEN p_sort IN ('TITLE','CANDIDATES') THEN count(DISTINCT title) ELSE count(DISTINCT amount) END
 INTO bad_count,variation FROM compared;
 IF bad_count<>0 OR variation<2 THEN RAISE EXCEPTION 'VARIED_TASK_SORT_WRONG_ORDER: % % % %',p_sort,p_direction,bad_count,variation;END IF;
 IF p_sort='PAYMENTS' AND (all_rows#>'{105,affected_payment_count}' IS DISTINCT FROM 'null'::jsonb
  OR all_rows#>'{105,affected_payment_count_complete}' IS DISTINCT FROM 'false'::jsonb) THEN
  RAISE EXCEPTION 'UNKNOWN_PAYMENT_COUNT_NOT_NULL_LAST';END IF;
END;
$sort$;
SELECT pg_temp.verify_varied_task_sort('TITLE','ASC');
SELECT pg_temp.verify_varied_task_sort('TITLE','DESC');
SELECT pg_temp.verify_varied_task_sort('CANDIDATES','ASC');
SELECT pg_temp.verify_varied_task_sort('CANDIDATES','DESC');
SELECT pg_temp.verify_varied_task_sort('PAYMENTS','ASC');
SELECT pg_temp.verify_varied_task_sort('PAYMENTS','DESC');
SELECT pg_temp.verify_varied_task_sort('AMOUNT','ASC');
SELECT pg_temp.verify_varied_task_sort('AMOUNT','DESC');
ROLLBACK TO SAVEPOINT varying_task_sorts;
-- Actual queued work is created only inside this disposable rollback fixture.
-- No work executor is invoked.105 separate source owners require continuation.
INSERT INTO public.candidates(id,display_name,tms_ref,pay_method)
SELECT ('10000000-0000-4000-8000-'||lpad((6000+n)::text,12,'0'))::uuid,'Task updating fixture '||n,'TASK-UPDATING-'||n,'PAYE'
FROM generate_series(1,105) n;
INSERT INTO public.banking_pay_workbench_session_scope(session_id,candidate_id,scope_ordinal,status,seeded,dirty)
SELECT '10000000-0000-4000-8000-000000000005'::uuid,('10000000-0000-4000-8000-'||lpad((6000+n)::text,12,'0'))::uuid,
 n+2,'FAILED',true,false FROM generate_series(1,105) n;
SELECT public.pay_workbench_enqueue_stage_continuation(p_session_id=>'10000000-0000-4000-8000-000000000005',
 p_candidate_id=>('10000000-0000-4000-8000-'||lpad((6000+n)::text,12,'0'))::uuid,
 p_job_type=>'WORKBENCH_CANDIDATE_LINE_WORK_PROCESS',p_actor_user_id=>'10000000-0000-4000-8000-000000000001',
 p_reason=>'DISPOSABLE_TASK_PAGE_PROOF',p_limit=>10) IS NOT NULL AS fixture_job_created FROM generate_series(1,105) n;
INSERT INTO task_page_results VALUES('with_updating',pg_temp.read_tasks());
INSERT INTO task_page_results VALUES('updating_first',pg_temp.read_tasks(p_view=>'UPDATING'));
INSERT INTO task_page_results SELECT 'updating_next',pg_temp.read_tasks(p_cursor=>payload->>'updating_next_cursor',p_view=>'UPDATING')
 FROM task_page_results WHERE label='with_updating';
INSERT INTO task_page_summaries VALUES('updating',pg_temp.read_task_summary());
DO $updating$
DECLARE a jsonb;b jsonb;c jsonb;
BEGIN
 SELECT payload INTO STRICT a FROM task_page_results WHERE label='with_updating';
 SELECT payload INTO STRICT b FROM task_page_results WHERE label='updating_first';
 SELECT payload INTO STRICT c FROM task_page_results WHERE label='updating_next';
 IF a->>'total_count'<>'105' OR a->>'updating_count'<>'105' OR jsonb_array_length(a->'updating')<>100
  OR a->'updating_has_more'<>'true'::jsonb OR jsonb_typeof(a->'updating_next_cursor')<>'string' THEN RAISE EXCEPTION 'UPDATING_INLINE_TRUNCATED_WITHOUT_CONTINUATION';END IF;
 IF b->>'view'<>'UPDATING' OR b->>'total_count'<>'105' OR b->>'scope_count'<>'105' OR b->'rows' IS DISTINCT FROM a->'updating'
  OR c->>'page_number'<>'2' OR jsonb_array_length(c->'rows')<>5 OR c->'has_more'<>'false'::jsonb THEN
  -- A direct UPDATING page carries the standard nullable presentation keys;
  -- the inline summary intentionally carries only the compact task core.
  IF b->>'view'<>'UPDATING' OR b->>'total_count'<>'105' OR b->>'scope_count'<>'105'
    OR (SELECT jsonb_agg(
          row_value
            - 'candidate_name' - 'candidate_reference' - 'payment_label'
            - 'payment_date' - 'affected_display_amount' - 'linked_timesheet_id'
          ORDER BY row_ordinal
        )
        FROM jsonb_array_elements(b->'rows') WITH ORDINALITY AS page_row(row_value,row_ordinal)
       ) IS DISTINCT FROM a->'updating'
    OR c->>'page_number'<>'2' OR jsonb_array_length(c->'rows')<>5 OR c->'has_more'<>'false'::jsonb THEN
    RAISE EXCEPTION 'UPDATING_CONTINUATION_LOST_TASKS';
  END IF;
 END IF;
 IF (SELECT count(DISTINCT r->>'identity') FROM jsonb_array_elements((b->'rows')||(c->'rows')) r)<>105
  OR EXISTS(SELECT 1 FROM jsonb_array_elements((b->'rows')||(c->'rows')) r WHERE r->>'issue_state'<>'UPDATING'
   OR r->>'title'<>'Refreshing…' OR r->'affected_payment_count'<>'null'::jsonb OR r->'affected_payment_count_complete'<>'false'::jsonb) THEN
  RAISE EXCEPTION 'UPDATING_UNKNOWN_COUNTS_OR_IDENTITIES';END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(a->'rows') r JOIN jsonb_array_elements(b->'rows') u ON r->>'identity'=u->>'identity') THEN
  RAISE EXCEPTION 'UPDATING_DUPLICATED_ACTION_TASK';END IF;
 IF EXISTS(SELECT 1 FROM task_page_results WHERE octet_length(convert_to(payload::text,'UTF8'))>256*1024) THEN
  RAISE EXCEPTION 'ACTION_RESPONSE_BUDGET_EXCEEDED';END IF;
 SET LOCAL client_min_messages='notice';
 RAISE NOTICE 'PASS: actual Action task paging105 tasks100/5 and40/40/25; stable sorts; literal search; stale/auth negatives; unchanged payments;105 Updating tasks with explicit continuation.';
END;
$updating$;
SELECT jsonb_build_object('label',r.label,'payload',r.payload,'summary',s.payload) FROM task_page_results r
 JOIN task_page_summaries s ON s.label=CASE WHEN r.label IN ('with_updating','updating_first','updating_next') THEN 'updating' ELSE 'initial' END
 ORDER BY r.label;
ROLLBACK;
