const assert=require('node:assert/strict');const test=require('node:test');const fs=require('node:fs');const path=require('node:path');
const {spawnSync}=require('node:child_process');const root=path.resolve(__dirname,'..');
test('candidate/page content evidence is generated over complete canonical facts before pagination or Timesheet compaction',()=>{
 const source=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_1159_banking_pay_modal_structure_v2.sql'),'utf8');
 const row=source.slice(source.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_modal_candidate_row_v2('),
  source.indexOf('ALTER FUNCTION private.pay_workbench_modal_candidate_row_v2('));
 const page=source.slice(source.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_modal_candidate_page_v2('),
  source.indexOf('ALTER FUNCTION private.pay_workbench_modal_candidate_page_v2('));
 assert.match(row,/'facts_digest'/);assert.match(row,/convert_to\(p_facts::text,'UTF8'\)/);
 assert.match(page,/'view_digest'/);
 assert.equal((page.match(/private\.pay_workbench_modal_candidate_facts_v2\(/g)||[]).length,1);
 assert.doesNotMatch(row+page,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});
test('actual full-scope summary and child content evidence agrees with private state and survives metadata-only renewal',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const fixture=fs.readFileSync(path.join(__dirname,'28082026_2038_banking_pay_summary_runtime.sql'),'utf8');
 const setup=fs.readFileSync(path.join(__dirname,'fixtures/28082026_1429_banking_pay_selection_setup.sql'),'utf8');
 assert.ok(setup.includes("current_database()<>'banking_modal_v2_test'"));assert.equal(fixture.split('DO $summary$').length,2);
 const prefix=fixture.slice(0,fixture.indexOf('DO $summary$')).replace('\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql',setup);
 assert.ok(!prefix.includes('\\ir'));
 const sql=`${prefix}
 DO $evidence$
 DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;state jsonb;first_page jsonb;next_page jsonb;
  child jsonb;old_row jsonb;new_row jsonb;expected_hash text;k text;d text;current_page jsonb;
  candidate uuid:='10000000-0000-4000-8000-000000000002';
 BEGIN
  SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
  opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
   'pay_channel_scope','ALL','scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'));
  state:=private.pay_workbench_modal_candidate_state_v2(s,'ALL',candidate);
  first_page:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,s.actor_user_id,'CANDIDATE','ASC',NULL,100);
  IF first_page->>'view_digest' IS DISTINCT FROM state->>'view_digest' THEN RAISE EXCEPTION 'SUMMARY_CONTENT_EVIDENCE_MISSING';END IF;
  next_page:=private.pay_workbench_modal_candidate_page_v2(s,opts,'CANDIDATE','ASC',first_page->>'next_cursor',100);
  IF next_page->>'view_digest' IS DISTINCT FROM state->>'view_digest' THEN RAISE EXCEPTION 'PAGE_CHANGED_CONTENT_EVIDENCE';END IF;
  FOREACH k IN ARRAY ARRAY['CANDIDATE','DEDUCTIONS','READY_TO_PAY'] LOOP
   FOREACH d IN ARRAY ARRAY['ASC','DESC'] LOOP
    current_page:=private.pay_workbench_modal_candidate_page_v2(s,opts,k,d,NULL,40);
    IF current_page->>'view_digest' IS DISTINCT FROM state->>'view_digest' THEN RAISE EXCEPTION 'SORT_CHANGED_CONTENT_EVIDENCE';END IF;
   END LOOP;
  END LOOP;
  SELECT r INTO STRICT old_row FROM jsonb_array_elements(first_page->'rows') r WHERE r->>'candidate_id'=candidate::text;
  SELECT encode(extensions.digest(convert_to(to_jsonb(f)::text,'UTF8'),'sha256'),'hex') INTO STRICT expected_hash
   FROM private.pay_workbench_modal_candidate_facts_v2(s,'ALL') f WHERE f.candidate_id=candidate;
  IF COALESCE(old_row->>'facts_digest','') !~ '^[a-f0-9]{64}$'
    OR old_row->>'facts_digest' IS DISTINCT FROM expected_hash
    OR old_row IS DISTINCT FROM state->'candidate' THEN RAISE EXCEPTION 'ROW_CONTENT_EVIDENCE_NOT_CANONICAL';END IF;
  child:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,candidate,opts,s.actor_user_id,NULL,100);
  IF child->'candidate' IS DISTINCT FROM old_row THEN RAISE EXCEPTION 'CHILD_CONTENT_EVIDENCE_NOT_CANONICAL';END IF;
  UPDATE public.banking_pay_workbench_sessions SET progress_counter_version=progress_counter_version+1 WHERE id=s.id RETURNING * INTO s;
  opts:=jsonb_set(opts,'{expected_progress_counter_version}',to_jsonb(s.progress_counter_version));
  current_page:=private.pay_workbench_modal_candidate_page_v2(s,opts,'CANDIDATE','ASC',NULL,100);
  SELECT r INTO STRICT new_row FROM jsonb_array_elements(current_page->'rows') r WHERE r->>'candidate_id'=candidate::text;
  IF current_page->>'view_digest' IS DISTINCT FROM state->>'view_digest'
    OR new_row->>'facts_digest' IS DISTINCT FROM expected_hash
    OR new_row->>'child_revision' IS NOT DISTINCT FROM old_row->>'child_revision'
    OR (new_row-'child_revision'-'selected_timesheet_scope_token') IS DISTINCT FROM
       (old_row-'child_revision'-'selected_timesheet_scope_token') THEN
   RAISE EXCEPTION 'METADATA_RENEWAL_CHANGED_CONTENT_PROOF';END IF;
  SET LOCAL client_min_messages='notice';
  RAISE NOTICE 'PASS: full-view and exact candidate facts agree across100/5 pages six sorts and current child; metadata-only renewal is distinct.';
 END;$evidence$;ROLLBACK;`;
 const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres',
  '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{cwd:root,input:sql,encoding:'utf8',timeout:60000,maxBuffer:1024*1024});
 assert.equal(r.status,0,r.stderr||r.error?.message);assert.match(r.stderr,/PASS: full-view and exact candidate facts/);
});
