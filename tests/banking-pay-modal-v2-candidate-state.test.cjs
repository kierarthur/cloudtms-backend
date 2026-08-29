const assert=require('node:assert/strict');const test=require('node:test');const fs=require('node:fs');const path=require('node:path');
const {spawnSync}=require('node:child_process');const root=path.resolve(__dirname,'..');
test('retention proof reads only existing complete candidate facts and never calculates pay',()=>{
 const sql=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_2245_banking_pay_modal_candidate_state.sql'),'utf8');
 for(const value of ['private.pay_workbench_modal_candidate_state_v2','private.pay_workbench_modal_candidate_facts_v2',
  'private.pay_workbench_modal_candidate_row_v2','view_digest','other_candidates_digest','membership_digest',
  "STABLE SECURITY INVOKER SET search_path TO ''"])assert.ok(sql.includes(value),value);
 assert.ok(!/\b(?:UPDATE|INSERT INTO|DELETE FROM)\s+public\.|\b(?:SUM|ROUND|ABS)\s*\(|GRANT EXECUTE/i.test(sql));
 assert.ok(!/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i.test(sql));
});
test('actual candidate changes, off-page names and exact selected Timesheets are reflected in the complete content proof',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const setup=fs.readFileSync(path.join(__dirname,'fixtures/28082026_1429_banking_pay_selection_setup.sql'),'utf8');
 assert.ok(setup.includes("current_database()<>'banking_modal_v2_test'"));
 const summaryFixture=fs.readFileSync(path.join(__dirname,'28082026_2038_banking_pay_summary_runtime.sql'),'utf8');
 assert.equal(summaryFixture.split('DO $summary$').length,2);
 const prefix=summaryFixture.slice(0,summaryFixture.indexOf('DO $summary$'))
  .replace('\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql',setup);
 assert.ok(!prefix.includes('\\ir'));
 const sql=`${prefix}
 DO $state$
 DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;before_state jsonb;after_state jsonb;changed jsonb;other_state jsonb;
  options jsonb;before_rows text;before_sessions text;old_name text;candidate uuid:='10000000-0000-4000-8000-000000000002';
  other_id uuid;timesheets jsonb;old_row jsonb;row_id uuid;current_base jsonb;first_page jsonb;second_page jsonb;
 BEGIN
  SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
  SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text) INTO before_rows FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id;
  before_sessions:=md5(to_jsonb(s)::text);
  before_state:=private.pay_workbench_modal_candidate_state_v2(s,'ALL',candidate);
  IF before_state->>'candidate_count' IS DISTINCT FROM '105' OR before_state->>'other_candidate_count' IS DISTINCT FROM '104'
   OR before_state#>>'{candidate,candidate_id}' IS DISTINCT FROM candidate::text THEN RAISE EXCEPTION 'RETENTION_SCOPE_MISSING';END IF;
  IF octet_length(convert_to(before_state::text,'UTF8'))>4096 THEN RAISE EXCEPTION 'RETENTION_METADATA_UNBOUNDED';END IF;
  IF before_rows IS DISTINCT FROM (SELECT md5(jsonb_agg(to_jsonb(r) ORDER BY r.id)::text) FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id)
   OR before_sessions IS DISTINCT FROM (SELECT md5(to_jsonb(q)::text) FROM public.banking_pay_workbench_sessions q WHERE id=s.id) THEN
   RAISE EXCEPTION 'RETENTION_READ_CHANGED_STATE';END IF;
  options:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
   'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
  changed:=public.pay_workbench_session_set_selected_rows(s.id,jsonb_build_object('modal_candidate_intent_v2',jsonb_build_object(
   'candidate_id',candidate,'request_id','10000000-0000-4000-8000-000000009999','action','CLEAR_ALL_READY','options',options)),s.actor_user_id);
  SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id=s.id;
  after_state:=private.pay_workbench_modal_candidate_state_v2(s,'ALL',candidate);
  IF after_state->>'view_digest'=before_state->>'view_digest'
   OR after_state->>'other_candidates_digest' IS DISTINCT FROM before_state->>'other_candidates_digest'
   OR after_state->>'membership_digest' IS DISTINCT FROM before_state->>'membership_digest'
   OR after_state#>>'{candidate,selection_state}' IS DISTINCT FROM 'NONE' THEN RAISE EXCEPTION 'RETENTION_MISCLASSIFIED_CANDIDATE_SELECTION';END IF;
  SELECT f.candidate_id INTO STRICT other_id FROM private.pay_workbench_modal_candidate_facts_v2(s,'ALL') f
   WHERE f.candidate_id<>candidate ORDER BY f.candidate_sort_name COLLATE "C" DESC,f.candidate_sort_reference COLLATE "C" DESC,f.candidate_id DESC LIMIT 1;
  options:=jsonb_set(options,'{expected_progress_counter_version}',to_jsonb(s.progress_counter_version));
  first_page:=private.pay_workbench_modal_candidate_page_v2(s,options,'CANDIDATE','ASC',NULL,100);
  second_page:=private.pay_workbench_modal_candidate_page_v2(s,options,'CANDIDATE','ASC',first_page->>'next_cursor',100);
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(first_page->'rows') q(r) WHERE r->>'candidate_id'=other_id::text)
   OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(second_page->'rows') q(r) WHERE r->>'candidate_id'=other_id::text) THEN
   RAISE EXCEPTION 'RETENTION_FIXTURE_NOT_OFF_PAGE';END IF;
  SELECT display_name INTO old_name FROM public.candidates WHERE id=other_id;
  UPDATE public.candidates SET display_name='Changed outside visible page' WHERE id=other_id;
  other_state:=private.pay_workbench_modal_candidate_state_v2(s,'ALL',candidate);
  IF other_state->>'other_candidates_digest'=after_state->>'other_candidates_digest' THEN RAISE EXCEPTION 'RETENTION_MISSED_CHANGED_NAME';END IF;
  UPDATE public.candidates SET display_name=old_name WHERE id=other_id;
  current_base:=private.pay_workbench_modal_candidate_state_v2(s,'ALL',candidate);
  SELECT id,row_json INTO STRICT row_id,old_row FROM public.banking_pay_workbench_preview_rows
   WHERE session_id=s.id AND candidate_id=other_id AND selected IS TRUE AND section='canonical_preview_lines' ORDER BY row_ordinal,id LIMIT 1;
  SELECT jsonb_agg(('00000000-0000-4000-8000-'||lpad(n::text,12,'0'))::uuid ORDER BY n) INTO timesheets FROM generate_series(1,35)n;
  UPDATE public.banking_pay_workbench_preview_rows SET row_json=old_row||jsonb_build_object('timesheet_ids',timesheets) WHERE id=row_id;
  other_state:=private.pay_workbench_modal_candidate_state_v2(s,'ALL',candidate);
  IF other_state->>'other_candidates_digest'=current_base->>'other_candidates_digest' THEN RAISE EXCEPTION 'RETENTION_MISSED_NON_INLINE_TIMESHEETS';END IF;
  current_base:=other_state;
  timesheets:=jsonb_set(timesheets,'{34}','"00000000-0000-4000-8000-000000009998"');
  UPDATE public.banking_pay_workbench_preview_rows SET row_json=old_row||jsonb_build_object('timesheet_ids',timesheets) WHERE id=row_id;
  other_state:=private.pay_workbench_modal_candidate_state_v2(s,'ALL',candidate);
  IF other_state->>'other_candidates_digest'=current_base->>'other_candidates_digest' THEN RAISE EXCEPTION 'RETENTION_MISSED_SAME_COUNT_CHANGED_TIMESHEETS';END IF;
  UPDATE public.banking_pay_workbench_preview_rows SET row_json=old_row WHERE id=row_id;
  other_state:=private.pay_workbench_modal_candidate_state_v2(s,'PAYE',candidate);
  IF other_state->>'view_digest'=after_state->>'view_digest' THEN RAISE EXCEPTION 'RETENTION_MISSED_SCOPE';END IF;
  s.progress_counter_version:=s.progress_counter_version+1;
  other_state:=private.pay_workbench_modal_candidate_state_v2(s,'ALL',candidate);
  IF other_state->>'view_digest' IS DISTINCT FROM after_state->>'view_digest'
    OR other_state#>>'{candidate,child_revision}'=after_state#>>'{candidate,child_revision}' THEN
   RAISE EXCEPTION 'RETENTION_CONFUSED_CONTENT_WITH_REVISION_METADATA';END IF;
  SET LOCAL client_min_messages='notice';
  RAISE NOTICE 'PASS: actual retention proof covers candidate selection; other names;35 selected Timesheets with same-count replacement; scope; revision metadata; no read writes.';
 END;$state$;ROLLBACK;`;
 const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres',
  '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{cwd:root,input:sql,encoding:'utf8',timeout:60000,maxBuffer:1024*1024});
 assert.equal(r.status,0,r.stderr||r.error?.message);assert.match(r.stderr,/PASS: actual retention proof/);
});

test('private content evidence rejects invalid context and describes empty or absent targets without inventing a candidate',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const setup=fs.readFileSync(path.join(__dirname,'fixtures/28082026_1429_banking_pay_selection_setup.sql'),'utf8');
 assert.ok(setup.includes("current_database()<>'banking_modal_v2_test'"));
 const sql=`BEGIN; SET LOCAL statement_timeout='30s'; SET LOCAL client_min_messages='warning';
 ${setup}
 DO $negative$
 DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;bad public.banking_pay_workbench_sessions%ROWTYPE;
  value jsonb;whole jsonb;other jsonb;channel text;k text;n integer;
 BEGIN
  SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
  FOREACH channel IN ARRAY ARRAY[NULL,'','all','INVALID'] LOOP
   BEGIN
    PERFORM private.pay_workbench_modal_candidate_state_v2(s,channel,NULL);
    RAISE EXCEPTION 'INVALID_CHANNEL_ACCEPTED';
   EXCEPTION WHEN invalid_parameter_value THEN
    IF SQLERRM IS DISTINCT FROM 'BANKING_PAY_V2_INVALID_INPUT' THEN RAISE;END IF;
   END;
  END LOOP;
  FOR n IN 1..7 LOOP
   bad:=s;
   CASE n WHEN 1 THEN bad.id:=NULL;WHEN 2 THEN bad.version:=NULL;WHEN 3 THEN bad.version:=-1;
    WHEN 4 THEN bad.progress_counter_version:=NULL;WHEN 5 THEN bad.progress_counter_version:=-1;
    WHEN 6 THEN bad.session_signature:=NULL;WHEN 7 THEN bad.pay_date:=NULL;END CASE;
   BEGIN
    PERFORM private.pay_workbench_modal_candidate_state_v2(bad,'ALL',NULL);
    RAISE EXCEPTION 'INVALID_CONTEXT_ACCEPTED %',n;
   EXCEPTION WHEN invalid_parameter_value THEN
    IF SQLERRM IS DISTINCT FROM 'BANKING_PAY_V2_INVALID_INPUT' THEN RAISE;END IF;
   END;
  END LOOP;
  whole:=private.pay_workbench_modal_candidate_state_v2(s,'ALL',NULL);
  other:=private.pay_workbench_modal_candidate_state_v2(s,'ALL','10000000-0000-4000-8000-000000009999');
  IF whole->'candidate' IS DISTINCT FROM 'null'::jsonb OR other->'candidate' IS DISTINCT FROM 'null'::jsonb
    OR whole->>'candidate_count' IS DISTINCT FROM whole->>'other_candidate_count'
    OR other->>'candidate_count' IS DISTINCT FROM other->>'other_candidate_count'
    OR other->>'view_digest' IS DISTINCT FROM whole->>'view_digest'
    OR other->>'membership_digest' IS DISTINCT FROM whole->>'membership_digest' THEN
   RAISE EXCEPTION 'ABSENT_TARGET_OR_WHOLE_SCOPE_NOT_EXACT';END IF;
  bad:=s;bad.id:='10000000-0000-4000-8000-000000009999';
  value:=private.pay_workbench_modal_candidate_state_v2(bad,'ALL',NULL);
  IF value->>'candidate_count' IS DISTINCT FROM '0' OR value->>'other_candidate_count' IS DISTINCT FROM '0'
    OR value->'candidate' IS DISTINCT FROM 'null'::jsonb THEN RAISE EXCEPTION 'EMPTY_SCOPE_NOT_EXACT';END IF;
  FOREACH k IN ARRAY ARRAY['view_digest','other_candidates_digest','membership_digest'] LOOP
   IF COALESCE(value->>k,'') !~ '^[a-f0-9]{64}$' OR COALESCE(whole->>k,'') !~ '^[a-f0-9]{64}$' THEN
    RAISE EXCEPTION 'MISSING_CONTENT_DIGEST %',k;END IF;
  END LOOP;
  SET LOCAL client_min_messages='notice';
  RAISE NOTICE 'PASS: private content context negatives and absent/empty scopes.';
 END;$negative$;ROLLBACK;`;
 const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres',
  '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{cwd:root,input:sql,encoding:'utf8',timeout:60000,maxBuffer:1024*1024});
 assert.equal(r.status,0,r.stderr||r.error?.message);assert.match(r.stderr,/PASS: private content context/);
});
