const assert=require('node:assert/strict');const fs=require('node:fs');const path=require('node:path');const test=require('node:test');
const {spawnSync}=require('node:child_process');const root=path.resolve(__dirname,'..');
test('main navigation remains a private bounded read without offsets or financial writes',()=>{
 const source=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_1159_banking_pay_modal_structure_v2.sql'),'utf8');
 const start=source.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_modal_candidate_page_v2(');
 const sql=source.slice(start,source.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_selected_ready_timesheets_v1(',start));
 assert.match(sql,/STABLE SECURITY INVOKER SET search_path TO ''/);
 assert.match(sql,/next_page_anchor/);assert.match(sql,/previous_page_anchor/);
 assert.match(sql,/v_navigation='STAY'/);assert.match(sql,/NOT IN \('STAY','NEXT','PREVIOUS'\)/);
 assert.doesNotMatch(sql,/\bOFFSET\b|\b(?:UPDATE|INSERT INTO|DELETE FROM)\s+public\./i);
 assert.doesNotMatch(sql,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
test('actual directed main-page anchors preserve navigation while ordinary cursors stay revision-strict',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const fixture=fs.readFileSync(path.join(__dirname,'28082026_1322_banking_pay_modal_candidate_paging.sql'),'utf8');
 const marker='DO $paging_proof$';assert.equal(fixture.split(marker).length,2);
 const setup=fixture.slice(0,fixture.indexOf(marker));assert.ok(setup.includes("current_database()<>'banking_modal_v2_test'"));
 const sql=setup+`
 DO $navigation$
 DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;a jsonb;b jsonb;c jsonb;bad jsonb;k text;d text;
  before_mode text:=current_setting('plan_cache_mode');
 BEGIN
  SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='00000000-0000-4000-8000-000000050003';
  opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
   'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
  FOREACH k IN ARRAY ARRAY['CANDIDATE','DEDUCTIONS','READY_TO_PAY'] LOOP
   FOREACH d IN ARRAY ARRAY['ASC','DESC'] LOOP
    a:=private.pay_workbench_modal_candidate_page_v2(s,opts,k,d,NULL,40);
    b:=private.pay_workbench_modal_candidate_page_v2(s,opts,k,d,a->>'next_cursor',40);
    c:=private.pay_workbench_modal_candidate_page_v2(s,opts,k,d,a->>'next_page_anchor',40);
    IF b IS DISTINCT FROM c THEN RAISE EXCEPTION 'SORTED_NEXT_ANCHOR_NOT_EQUIVALENT % %',k,d;END IF;
    c:=private.pay_workbench_modal_candidate_page_v2(s,opts,k,d,b->>'previous_page_anchor',40);
    IF a IS DISTINCT FROM c THEN RAISE EXCEPTION 'SORTED_PREVIOUS_ANCHOR_NOT_EQUIVALENT % %',k,d;END IF;
   END LOOP;
  END LOOP;
  a:=private.pay_workbench_modal_candidate_page_v2(s,opts,'CANDIDATE','ASC',NULL,100);
  b:=private.pay_workbench_modal_candidate_page_v2(s,opts,'CANDIDATE','ASC',a->>'next_cursor',100);
  IF a->>'next_page_anchor' IS NULL OR a->'previous_page_anchor'<>'null'::jsonb
   OR b->>'previous_page_anchor' IS NULL OR b->'next_page_anchor'<>'null'::jsonb THEN RAISE EXCEPTION 'MAIN_DIRECTED_ANCHORS_MISSING';END IF;
  c:=private.pay_workbench_modal_candidate_page_v2(s,opts,'CANDIDATE','ASC',a->>'next_page_anchor',100);
  IF c IS DISTINCT FROM b THEN RAISE EXCEPTION 'MAIN_NEXT_ANCHOR_NOT_EQUIVALENT';END IF;
  UPDATE public.banking_pay_workbench_sessions SET progress_counter_version=progress_counter_version+1 WHERE id=s.id RETURNING * INTO s;
  opts:=jsonb_set(opts,'{expected_progress_counter_version}',to_jsonb(s.progress_counter_version));
  BEGIN
   PERFORM private.pay_workbench_modal_candidate_page_v2(s,opts,'CANDIDATE','ASC',a->>'next_cursor',100);
   RAISE EXCEPTION 'ORDINARY_CURSOR_RENEWED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE;END IF;END;
  c:=private.pay_workbench_modal_candidate_page_v2(s,opts,'CANDIDATE','ASC',a->>'next_page_anchor',100);
  IF c->>'page_number'<>'2' OR jsonb_array_length(c->'rows')<>5 OR c#>>'{rows,0,candidate_id}'<>b#>>'{rows,0,candidate_id}'
   THEN RAISE EXCEPTION 'MAIN_NEXT_ANCHOR_LOST_POSITION';END IF;
  c:=private.pay_workbench_modal_candidate_page_v2(s,opts,'CANDIDATE','ASC',b->>'previous_page_anchor',100);
  IF c->>'page_number'<>'1' OR jsonb_array_length(c->'rows')<>100 OR c#>>'{rows,0,candidate_id}'<>a#>>'{rows,0,candidate_id}'
   THEN RAISE EXCEPTION 'MAIN_PREVIOUS_ANCHOR_LOST_POSITION';END IF;
  bad:=private.pay_workbench_modal_cursor_decode_v2(a->>'next_page_anchor','{}');
  BEGIN
   PERFORM private.pay_workbench_modal_candidate_page_v2(s,opts,'CANDIDATE','ASC',
    private.pay_workbench_modal_cursor_encode_v2(bad||'{"navigation":"SIDEWAYS"}'::jsonb),100);
   RAISE EXCEPTION 'BAD_NAVIGATION_ACCEPTED';
  EXCEPTION WHEN invalid_parameter_value THEN IF SQLERRM<>'BANKING_PAY_V2_INVALID_CURSOR' THEN RAISE;END IF;END;
  BEGIN
   PERFORM private.pay_workbench_modal_candidate_page_v2(s,opts,'CANDIDATE','ASC',
    private.pay_workbench_modal_cursor_encode_v2(bad||'{"progress_counter_version":999}'::jsonb),100);
   RAISE EXCEPTION 'FUTURE_NAVIGATION_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE;END IF;END;
  FOREACH k IN ARRAY ARRAY['scope_hash','session_version','sort_key','sort_direction','page_limit'] LOOP
   BEGIN
    PERFORM private.pay_workbench_modal_candidate_page_v2(s,opts,'CANDIDATE','ASC',
     private.pay_workbench_modal_cursor_encode_v2(jsonb_set(bad,ARRAY[k],'"different"'::jsonb)),100);
    RAISE EXCEPTION 'CROSS_SCOPE_NAVIGATION_ACCEPTED';
   EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE;END IF;END;
  END LOOP;
  bad:=private.pay_workbench_modal_cursor_decode_v2(a->>'page_anchor','{}')||'{"navigation":"PREVIOUS"}'::jsonb;
  BEGIN
   PERFORM private.pay_workbench_modal_candidate_page_v2(s,opts,'CANDIDATE','ASC',private.pay_workbench_modal_cursor_encode_v2(bad),100);
   RAISE EXCEPTION 'BEFORE_FIRST_PAGE_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE;END IF;END;
  bad:=private.pay_workbench_modal_cursor_decode_v2(b->>'page_anchor','{}')||'{"navigation":"NEXT"}'::jsonb;
  BEGIN
   PERFORM private.pay_workbench_modal_candidate_page_v2(s,opts,'CANDIDATE','ASC',private.pay_workbench_modal_cursor_encode_v2(bad),100);
   RAISE EXCEPTION 'AFTER_LAST_PAGE_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE;END IF;END;
  UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json||'{"presentation_role":"HIDDEN_INDEFINITE_SNOOZE"}'::jsonb
   WHERE session_id=s.id AND candidate_id=(a#>>'{rows,0,candidate_id}')::uuid;
  UPDATE public.banking_pay_workbench_sessions SET progress_counter_version=progress_counter_version+1 WHERE id=s.id RETURNING * INTO s;
  opts:=jsonb_set(opts,'{expected_progress_counter_version}',to_jsonb(s.progress_counter_version));
  c:=private.pay_workbench_modal_candidate_page_v2(s,opts,'CANDIDATE','ASC',a->>'next_page_anchor',100);
  IF c->>'total_count'<>'104' OR c->>'page_number'<>'2' OR jsonb_array_length(c->'rows')<>4 THEN
   RAISE EXCEPTION 'DEPARTED_BOUNDARY_NAVIGATION_LOST_ROWS';END IF;
  SET LOCAL client_min_messages='notice';
  IF current_setting('plan_cache_mode') IS DISTINCT FROM before_mode THEN RAISE EXCEPTION 'NAVIGATION_CHANGED_CALLER_PLANNING';END IF;
  RAISE NOTICE 'PASS: directed main anchors105 candidates across current revisions; ordinary cursors strict; departed boundary and negative scopes.';
 END;
 $navigation$;
 ROLLBACK;
 `;
 const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres',
  '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{cwd:root,input:sql,encoding:'utf8',timeout:45000,maxBuffer:2*1024*1024});
 assert.equal(r.status,0,r.stderr||r.error?.message);assert.match(r.stderr,/PASS: directed main anchors/);
});
