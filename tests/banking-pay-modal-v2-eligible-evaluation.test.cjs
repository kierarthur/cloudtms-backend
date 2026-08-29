const assert=require('node:assert/strict');const test=require('node:test');const fs=require('node:fs');const path=require('node:path');
const {spawnSync}=require('node:child_process');const root=path.resolve(__dirname,'..');
const source=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_1232_banking_pay_modal_certified_projection.sql'),'utf8').replaceAll('\r\n','\n');
const start=source.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_modal_eligible_rows_v2(');
const definition=source.slice(start,source.indexOf('ALTER FUNCTION private.pay_workbench_modal_eligible_rows_v2(',start));
const barrier='), eligible_rows AS MATERIALIZED (';
test('the complete certified eligible set is evaluated before the final physical-row join',()=>{
 assert.equal(definition.split(barrier).length,2);
 assert.match(definition,/JOIN eligible_rows AS eligible_row ON eligible_row.id = source_row.id/);
 assert.match(definition,/LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path TO ''/);
 assert.doesNotMatch(definition,/\b(?:INSERT INTO|UPDATE public\.|DELETE FROM)\b/i);
});
test('actual old/new eligible rows agree around selection and post-Draft fences, with bounded repeated section checks under nested loops',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 assert.equal(definition.split(barrier).length,2);
 const original=definition.replace(barrier,'), eligible_rows AS (')
  .replace('private.pay_workbench_modal_eligible_rows_v2(', 'pg_temp.original_eligible_v2(');
 const setup=fs.readFileSync(path.join(__dirname,'fixtures/28082026_1429_banking_pay_selection_setup.sql'),'utf8');
 assert.ok(setup.includes("current_database()<>'banking_modal_v2_test'"));
 const sql=`BEGIN;SET LOCAL statement_timeout='30s';SET LOCAL client_min_messages='warning';
 ${setup}
 ${original}
 CREATE FUNCTION pg_temp.compare_eligible_v2(s public.banking_pay_workbench_sessions) RETURNS void LANGUAGE plpgsql AS $compare$
 DECLARE section text;old_rows jsonb;new_rows jsonb;
 BEGIN
  FOREACH section IN ARRAY ARRAY['canonical_preview_lines','cases_resolutions','blocked_for_pay'] LOOP
   SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.id),'[]'::jsonb) INTO old_rows FROM pg_temp.original_eligible_v2(s.id,s.version,section) r;
   SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.id),'[]'::jsonb) INTO new_rows FROM private.pay_workbench_modal_eligible_rows_v2(s.id,s.version,section) r;
   IF old_rows IS DISTINCT FROM new_rows THEN RAISE EXCEPTION 'ELIGIBLE_ROWS_DRIFT %',section;END IF;
  END LOOP;
 END;$compare$;
 DO $parity$
 DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;result jsonb;row_id uuid;section text;action text;
  before_calls bigint;after_calls bigint;source_count bigint;
 BEGIN
  SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
  PERFORM pg_temp.compare_eligible_v2(s);
  -- The setup starts with Blocked recoveries. First promote them, then prove
  -- demotion as well; clearing that initial fixture alone cannot move them.
  FOREACH action IN ARRAY ARRAY['SELECT_ALL_READY','CLEAR_ALL_READY'] LOOP
   opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
    'pay_channel_scope','ALL','scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'));
   result:=public.pay_workbench_session_set_selected_rows(s.id,jsonb_build_object('modal_candidate_intent_v2',jsonb_build_object(
    'candidate_id','10000000-0000-4000-8000-000000000002',
    'request_id',CASE action WHEN 'SELECT_ALL_READY' THEN '10000000-0000-4000-8000-000000009998' ELSE '10000000-0000-4000-8000-000000009999' END,
    'action',action,'options',opts)),s.actor_user_id);
   SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id=s.id;
   IF result->>'state_changed' IS DISTINCT FROM 'true' OR COALESCE(jsonb_array_length(result->'movements'),0)<1 THEN
    RAISE EXCEPTION 'ELIGIBILITY_FIXTURE_MUST_MOVE_RECOVERY %',action;END IF;
   PERFORM pg_temp.compare_eligible_v2(s);
  END LOOP;
  SELECT id INTO STRICT row_id FROM public.banking_pay_workbench_preview_rows WHERE session_id=s.id AND row_ordinal=1;
  UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json||'{"post_draft_unavailable":true}'::jsonb WHERE id=row_id;
  PERFORM pg_temp.compare_eligible_v2(s);
  IF EXISTS(SELECT 1 FROM private.pay_workbench_modal_eligible_rows_v2(s.id,s.version,'canonical_preview_lines') r WHERE r.id=row_id) THEN
   RAISE EXCEPTION 'POST_DRAFT_EXCLUSION_LOST';END IF;
  PERFORM set_config('track_functions','all',true);
  PERFORM set_config('enable_hashjoin','off',true);PERFORM set_config('enable_mergejoin','off',true);
  SELECT count(*) INTO source_count FROM public.banking_pay_workbench_preview_rows WHERE session_id=s.id;
  SELECT COALESCE(sum(calls),0) INTO before_calls FROM pg_stat_xact_user_functions WHERE funcname='pay_workbench_preview_effective_section_v1';
  FOREACH section IN ARRAY ARRAY['canonical_preview_lines','cases_resolutions','blocked_for_pay'] LOOP
   PERFORM count(*) FROM private.pay_workbench_modal_eligible_rows_v2(s.id,s.version,section);
  END LOOP;
  SELECT COALESCE(sum(calls),0) INTO after_calls FROM pg_stat_xact_user_functions WHERE funcname='pay_workbench_preview_effective_section_v1';
  IF after_calls<=before_calls OR after_calls-before_calls>32*(source_count+1) THEN
   RAISE EXCEPTION 'ELIGIBILITY_CHECKS_REPEATED_QUADRATICALLY %, rows%',after_calls-before_calls,source_count;END IF;
  SET LOCAL client_min_messages='notice';
  RAISE NOTICE 'PASS: exact eligibility before/after actual recovery movement and post-Draft fence; nested-loop section calls%, rows%.',
   after_calls-before_calls,source_count;
 END;$parity$;ROLLBACK;`;
 const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres',
  '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{input:sql,encoding:'utf8',timeout:60000,maxBuffer:1024*1024});
 assert.equal(r.status,0,r.stderr||r.error?.message);assert.match(r.stderr,/PASS: exact eligibility/);
});
