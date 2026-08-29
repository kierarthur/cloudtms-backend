const assert=require('node:assert/strict');
const test=require('node:test');
const fs=require('node:fs');
const path=require('node:path');
const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');
const fixture=fs.readFileSync(path.join(__dirname,'28082026_1254_banking_pay_modal_ready_page_runtime.sql'),'utf8');
const setup=fixture.slice(0,fixture.indexOf('DO $ready_read_proof$'));
const source=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_1159_banking_pay_modal_structure_v2.sql'),'utf8');
const reader=source.slice(source.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_selected_ready_timesheets_v1('),
  source.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_candidate_ready_page_v1('));
const enabled=Boolean(process.env.BANKING_MODAL_LOCAL_PSQL);

test('Timesheet renewal retains exact current request validation and recomputes all selected IDs',()=>{
  assert.equal((reader.match(/private\.pay_workbench_modal_context_v2\(/g)||[]).length,2);
  assert.match(reader,/pay_workbench_modal_candidate_facts_v2/);
  assert.match(reader,/timesheet_hash.*IS DISTINCT FROM v_hash/);
  assert.doesNotMatch(reader.slice(reader.indexOf('AS $function$'),reader.indexOf('$function$;')),/\b(?:UPDATE|INSERT|DELETE|EXECUTE)\b.*\bpublic\./i);
  assert.doesNotMatch(reader,/LIMIT\s+25|pay_batch|provider_submit/i);
});

test('unchanged selected-only membership renews; stale membership and every binding negative reject',{skip:!enabled},()=>{
  const sql=setup+`
DO $shortcut_proof$
DECLARE
  s public.banking_pay_workbench_sessions%ROWTYPE;
  opts jsonb; binding jsonb; facts record; row_data jsonb; token text; token_data jsonb;
  result jsonb; bad jsonb; original_ids jsonb; before_rows text; after_rows text;
BEGIN
  SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id='00000000-0000-4000-8000-000000000005';
  opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
    'pay_channel_scope','ALL','scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'));
  binding:=jsonb_build_object('contract','BANKING_PAY_MODAL_STRUCTURE_V2','session_id',s.id,
    'session_version',s.version,'progress_counter_version',s.progress_counter_version,'scope_hash',opts->>'scope_hash');
  SELECT * INTO facts FROM private.pay_workbench_modal_candidate_facts_v2(s,'ALL')
    WHERE candidate_id='00000000-0000-4000-8000-000000000002';
  row_data:=private.pay_workbench_modal_candidate_row_v2(to_jsonb(facts),binding);
  token:=row_data->>'selected_timesheet_scope_token';
  token_data:=private.pay_workbench_modal_cursor_decode_v2(token,'{}');
  original_ids:=to_jsonb(facts.selected_timesheet_ids);
  -- Simulate only an unrelated candidate's selection revision; never a hosted mutation.
  UPDATE public.banking_pay_workbench_preview_rows SET selected=NOT selected
    WHERE session_id=s.id AND candidate_id='00000000-0000-4000-8000-000000000003';
  UPDATE public.banking_pay_workbench_sessions SET progress_counter_version=progress_counter_version+1 WHERE id=s.id RETURNING * INTO s;
  BEGIN
    PERFORM public.pay_workbench_session_get_selected_ready_timesheets_v1(s.id,facts.candidate_id,opts,s.actor_user_id,token);
    RAISE EXCEPTION 'STALE_REQUEST_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_REVISION' THEN RAISE; END IF; END;
  opts:=opts||jsonb_build_object('expected_progress_counter_version',s.progress_counter_version);
  SELECT md5(string_agg(to_jsonb(r)::text,'' ORDER BY r.id)) INTO before_rows
    FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id;
  result:=public.pay_workbench_session_get_selected_ready_timesheets_v1(s.id,facts.candidate_id,opts,s.actor_user_id,token);
  IF result->'timesheet_ids' IS DISTINCT FROM original_ids OR result->>'timesheet_count'<>'35'
    OR result->>'progress_counter_version' IS DISTINCT FROM s.progress_counter_version::text THEN
    RAISE EXCEPTION 'UNCHANGED_MEMBERSHIP_MUST_RETURN_COMPLETE_CURRENT_IDS';
  END IF;
  FOR bad IN SELECT value FROM jsonb_array_elements(jsonb_build_array(
      token_data-'progress_counter_version',
      token_data||'{"progress_counter_version":null}',
      token_data||'{"progress_counter_version":"4"}',
      token_data||'{"progress_counter_version":-1}',
      token_data||'{"progress_counter_version":4.5}',
      token_data||'{"progress_counter_version":99999999999999999999999}',
      token_data||jsonb_build_object('progress_counter_version',s.progress_counter_version+1),
      token_data||'{"kind":"CANDIDATE_PAGE_ANCHOR"}',
      token_data||'{"candidate_id":"00000000-0000-4000-8000-000000000003"}',
      token_data||'{"session_id":"00000000-0000-4000-8000-000000000099"}',
      token_data||'{"session_version":2}',
      token_data||'{"scope_hash":"wrong"}',
      token_data||'{"timesheet_hash":"wrong"}'
    ))
  LOOP
    BEGIN
      PERFORM public.pay_workbench_session_get_selected_ready_timesheets_v1(s.id,facts.candidate_id,opts,s.actor_user_id,
        private.pay_workbench_modal_cursor_encode_v2(bad));
      RAISE EXCEPTION 'INVALID_TOKEN_ACCEPTED';
    EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE; END IF; END;
  END LOOP;
  BEGIN
    PERFORM private.pay_workbench_modal_cursor_decode_v2(token,
      binding||jsonb_build_object('progress_counter_version',s.progress_counter_version));
    RAISE EXCEPTION 'ORDINARY_CURSOR_REVISION_WAS_WEAKENED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE; END IF; END;
  SELECT md5(string_agg(to_jsonb(r)::text,'' ORDER BY r.id)) INTO after_rows
    FROM public.banking_pay_workbench_preview_rows r WHERE session_id=s.id;
  IF before_rows IS DISTINCT FROM after_rows THEN RAISE EXCEPTION 'SHORTCUT_READ_MUTATED_ROWS'; END IF;
  -- Same count but different IDs must also reject, not just changed count.
  UPDATE public.banking_pay_workbench_preview_rows
    SET selected=(row_ordinal=4),selection_state=CASE WHEN row_ordinal=4 THEN 'SELECTED' ELSE 'UNSELECTED' END
    WHERE session_id=s.id AND candidate_id=facts.candidate_id AND row_ordinal IN (3,4);
  BEGIN
    PERFORM public.pay_workbench_session_get_selected_ready_timesheets_v1(s.id,facts.candidate_id,opts,s.actor_user_id,token);
    RAISE EXCEPTION 'CHANGED_EQUAL_COUNT_MEMBERSHIP_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_STALE_CURSOR' THEN RAISE; END IF; END;
  RAISE NOTICE 'PASS: current selected-only membership renewal and all stale bindings';
END
$shortcut_proof$;
ROLLBACK;
`;
  const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
    {input:sql,cwd:root,encoding:'utf8',timeout:60000});
  assert.equal(result.status,0,result.stderr||result.error?.message);
  assert.match(result.stderr,/PASS: current selected-only membership renewal/);
});

test('existing complete Candidate Ready and selected Timesheet fixture still passes',{skip:!enabled},()=>{
  const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
    {input:fixture,cwd:root,encoding:'utf8',timeout:60000});
  assert.equal(result.status,0,result.stderr||result.error?.message);
});
