const assert=require('node:assert/strict');const test=require('node:test');const fs=require('node:fs');const path=require('node:path');
const {spawnSync}=require('node:child_process');const root=path.resolve(__dirname,'..');
const source=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_1232_banking_pay_modal_certified_projection.sql'),'utf8').replaceAll('\r\n','\n');
const start=source.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_modal_row_payload_v2(');
const definition=source.slice(start,source.indexOf('ALTER FUNCTION private.pay_workbench_modal_row_payload_v2(',start));
const barrier='      -- Single-row evaluation barrier; not list pagination.\n      OFFSET 0\n';
test('only the single-row base JSON subquery receives the constant evaluation barrier',()=>{
 assert.equal(definition.split(barrier).length,2);
 assert.equal((definition.match(/\bOFFSET\b/g)||[]).length,1);
 assert.match(definition,/AS base_json\n      -- Single-row evaluation barrier; not list pagination.\n      OFFSET 0\n    \) AS base_values/);
 assert.match(definition,/LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path TO ''/);
 assert.doesNotMatch(definition,/SET (?:plan_cache_mode|statement_timeout)|\b(?:UPDATE|INSERT INTO|DELETE FROM)\b/);
});
test('actual old/new payload outputs agree across null shapes boolean spellings effective sections and post-Draft overlays',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 assert.equal(definition.split(barrier).length,2);
 const original=definition.replace(barrier,'').replace('private.pay_workbench_modal_row_payload_v2(', 'pg_temp.original_payload_v2(');
 const sql=`BEGIN; SET LOCAL statement_timeout='30s';
 DO $target$ BEGIN
  IF current_database() IS DISTINCT FROM 'banking_modal_v2_test'
    OR current_setting('server_version_num')::int NOT BETWEEN 170000 AND 179999 THEN RAISE EXCEPTION 'WRONG_LOCAL_TARGET';END IF;
 END;$target$;
 ${original}
 CREATE TEMP TABLE payload_samples ON COMMIT DROP AS
 SELECT row_number() OVER() AS n,jsonb_populate_record(NULL::public.banking_pay_workbench_preview_rows,
  jsonb_build_object('id','10000000-0000-4000-8000-000000000001','candidate_id','10000000-0000-4000-8000-000000000002',
   'section',section,'selected',selected,'selection_state',CASE WHEN selected THEN 'SELECTED' ELSE 'UNSELECTED' END,
   'status','READY','session_version',1,'row_key','payload-sample','row_ordinal',1,'key_type','SOURCE_REF','key_value','payload-sample',
   'timesheet_id','10000000-0000-4000-8000-000000000003','row_json',payload)) AS source_row
 FROM unnest(ARRAY['canonical_preview_lines','cases_resolutions','blocked_for_pay','hidden']) section
 CROSS JOIN unnest(ARRAY[true,false,NULL::boolean]) selected
 CROSS JOIN jsonb_array_elements('[null,[],false,{}]'::jsonb||
  (SELECT jsonb_agg(jsonb_build_object('amount_ex_vat',amount,'amount_display',amount,
   'section_amount_display','-12.30','selection_allowed',flag,'draftable',flag,'is_ready_for_draft',flag,
   'is_excluded_from_allocation',flag,'is_recognised_finance_deduction',flag,'materialisable',flag,
   'post_draft_unavailable',overlay,'post_draft_overlay_applied',overlay,
   'post_draft_overlay_operation_type','DRAFT_CREATE','post_draft_overlay_active',overlay,
   'candidate_display_name','  O''Connor – £ fixture  ','candidate_name',NULL,'preview_contract',contract))
   FROM unnest(ARRAY['true','false','YES','no','1','0','','unknown']) flag
   CROSS JOIN unnest(ARRAY['true','false',NULL::text]) overlay
   CROSS JOIN unnest(ARRAY['0.00','-12.30','17.456','not numeric']) amount
   CROSS JOIN unnest(ARRAY['{}'::jsonb,'null'::jsonb,'[]'::jsonb]) contract)) payload;
 DO $parity$
 DECLARE bad bigint;
 BEGIN
  IF (SELECT count(*) FROM payload_samples) IS DISTINCT FROM 3504::bigint THEN RAISE EXCEPTION 'INSUFFICIENT_PAYLOAD_FIXTURES';END IF;
  SELECT n INTO bad FROM payload_samples r
   WHERE pg_temp.original_payload_v2(r.source_row) IS DISTINCT FROM private.pay_workbench_modal_row_payload_v2(r.source_row) LIMIT 1;
  IF bad IS NOT NULL THEN RAISE EXCEPTION 'PAYLOAD_NORMALIZATION_DRIFT sample%',bad;END IF;
  IF pg_temp.original_payload_v2(NULL::public.banking_pay_workbench_preview_rows)
   IS DISTINCT FROM private.pay_workbench_modal_row_payload_v2(NULL::public.banking_pay_workbench_preview_rows) THEN
   RAISE EXCEPTION 'NULL_ROW_NORMALIZATION_DRIFT';END IF;
  RAISE NOTICE 'PASS:3504 complete payload normalization fixtures and null composite unchanged.';
 END;$parity$;ROLLBACK;`;
 const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres',
  '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{input:sql,encoding:'utf8',timeout:60000,maxBuffer:1024*1024});
 assert.equal(r.status,0,r.stderr||r.error?.message);assert.match(r.stderr,/PASS:3504/);
});
