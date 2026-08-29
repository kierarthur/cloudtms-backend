// Synthetic disposable PostgreSQL17 diagnosis only. Not a hosted/browser SLA.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {spawnSync} from 'node:child_process';
const fixture=fs.readFileSync('tests/28082026_2038_banking_pay_summary_runtime.sql','utf8');
let prefix=fixture.slice(0,fixture.indexOf('DO $summary$'));
const setup=fs.readFileSync('tests/fixtures/28082026_1429_banking_pay_selection_setup.sql','utf8');
assert.ok(setup.includes("current_database()<>'banking_modal_v2_test'"));
prefix=prefix.replace('\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql',setup);
assert.ok(!prefix.includes('\\ir'));assert.ok(process.env.BANKING_MODAL_LOCAL_PSQL);
const planMode=process.argv[2]||'auto';
assert.ok(['auto','force_custom_plan','force_generic_plan'].includes(planMode));
const scope=process.argv[3]||'session';
assert.ok(['session','eligible','summary','payload','eligible_plpgsql','payload_once'].includes(scope));
const projection=fs.readFileSync('supabase/repeatable/28082026_1232_banking_pay_modal_certified_projection.sql','utf8');
const eligibleDefinition=projection.slice(projection.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_modal_eligible_rows_v2('),
 projection.indexOf('ALTER FUNCTION private.pay_workbench_modal_eligible_rows_v2('));
assert.ok(eligibleDefinition.includes('SELECT source_row.*'));
const plpgsqlDefinition=eligibleDefinition.includes('LANGUAGE plpgsql')?eligibleDefinition:eligibleDefinition.replace('LANGUAGE sql','LANGUAGE plpgsql')
 .replace('AS $function$','AS $function$\nBEGIN\n  RETURN QUERY')
 .replace('$function$;','END;\n$function$;');
const payloadDefinition=projection.slice(projection.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_modal_row_payload_v2('),
 projection.indexOf('ALTER FUNCTION private.pay_workbench_modal_row_payload_v2('))
 .replace('      -- Single-row evaluation barrier; not list pagination.\n      OFFSET 0\n','');
const baseMarker='        ) AS base_json\n    ) AS base_values';
assert.equal(payloadDefinition.split(baseMarker).length,2);
const payloadOnce=payloadDefinition.replace(baseMarker,'        ) AS base_json\n      OFFSET 0\n    ) AS base_values');
const sql=prefix+`
SET LOCAL client_min_messages='notice';
SET LOCAL plan_cache_mode='${planMode}';
${scope==='eligible'?"ALTER FUNCTION private.pay_workbench_modal_eligible_rows_v2(uuid,bigint,text) SET plan_cache_mode='force_custom_plan';":''}
${scope==='summary'?"ALTER FUNCTION public.pay_workbench_session_get_candidate_summary_page_v1(uuid,jsonb,uuid,text,text,text,integer) SET plan_cache_mode='force_custom_plan';":''}
${scope==='payload'?"ALTER FUNCTION private.pay_workbench_modal_row_payload_v2(public.banking_pay_workbench_preview_rows) SET plan_cache_mode='force_generic_plan';":''}
${scope==='eligible_plpgsql'?plpgsqlDefinition:''}
${scope==='payload_once'?payloadDefinition.replace('private.pay_workbench_modal_row_payload_v2(', 'pg_temp.original_payload_v2(')+payloadOnce+`
 DO $payload_parity$ BEGIN
  IF EXISTS(SELECT 1 FROM public.banking_pay_workbench_preview_rows r
   WHERE r.session_id='10000000-0000-4000-8000-000000000005'
    AND pg_temp.original_payload_v2(r) IS DISTINCT FROM private.pay_workbench_modal_row_payload_v2(r)) THEN
   RAISE EXCEPTION 'PROFILE_PAYLOAD_SEMANTICS_CHANGED';END IF;
 END;$payload_parity$;`:''}
SET LOCAL track_functions='all';
DO $profile$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;opts jsonb;p jsonb;t timestamptz;j text;n integer;v_result jsonb;v_hash text;
BEGIN
 SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
 opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
 'scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'),'pay_channel_scope','ALL');
 p:=private.pay_workbench_modal_draft_gate_v2(s.id,212);
 FOREACH j IN ARRAY ARRAY['on','off'] LOOP
  PERFORM set_config('jit',j,true);
  FOR n IN 1..3 LOOP
   t:=clock_timestamp();
   PERFORM * FROM private.pay_workbench_modal_source_issue_members_v2(s,'ALL',p,true);
   RAISE NOTICE 'PROFILE jit=% sample=% source_issue_ms=%',j,n,round(extract(epoch FROM clock_timestamp()-t)*1000,2);
   t:=clock_timestamp();
   PERFORM * FROM private.pay_workbench_modal_issue_index_v2(s,'ALL','CSV','PROD',p,true,true);
   RAISE NOTICE 'PROFILE jit=% sample=% issue_index_ms=%',j,n,round(extract(epoch FROM clock_timestamp()-t)*1000,2);
   t:=clock_timestamp();
   v_result:=public.pay_workbench_session_get_candidate_summary_page_v1(s.id,opts,s.actor_user_id);
   IF v_hash IS NOT NULL AND v_hash<>md5(v_result::text) THEN RAISE EXCEPTION 'PROFILE_CHANGED_RESULT';END IF;
   v_hash:=md5(v_result::text);
   RAISE NOTICE 'PROFILE jit=% sample=% summary_ms=%',j,n,round(extract(epoch FROM clock_timestamp()-t)*1000,2);
  END LOOP;
 END LOOP;
END;
$profile$;
SELECT jsonb_build_object('function',schemaname||'.'||funcname,'calls',calls,
 'total_ms',round(total_time::numeric,2),'self_ms',round(self_time::numeric,2))
FROM pg_stat_xact_user_functions WHERE calls>0 ORDER BY total_time DESC LIMIT 15;
ROLLBACK;`;
const result=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres',
 '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{input:sql,encoding:'utf8',timeout:60000,maxBuffer:1024*1024});
if(result.error)throw result.error;
console.log(result.stderr.split(/\r?\n/).filter(line=>line.includes('PROFILE')).join('\n'));
console.log(result.stdout);
assert.equal(result.status,0,result.error?.message||result.stderr);
