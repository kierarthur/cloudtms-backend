const assert=require('node:assert/strict');const test=require('node:test');const fs=require('node:fs');const path=require('node:path');
const localQuery=require('./fixtures/banking-pay-local-query.cjs');
const file=path.join(__dirname,'../supabase/repeatable/28082026_2234_banking_pay_modal_movement_envelope.sql');
test('movement projection is a private pure formatter and cannot change financial owners',()=>{
 const sql=fs.readFileSync(file,'utf8');
 for(const expected of ['private.pay_workbench_modal_movement_envelope_v2','IMMUTABLE SECURITY INVOKER',"SET search_path TO ''",
  'movements_complete','movement_count','movement_digest','ALL_PREVIOUS_DETAILS','8192'])assert.ok(sql.includes(expected),expected);
 assert.ok(!/\b(?:UPDATE|INSERT INTO|DELETE FROM)\s+public\.|\b(?:GRANT|CREATE TABLE)\b/i.test(sql));
 assert.ok(!/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i.test(sql));
});
test('actual movement projection is complete when compact and explicitly invalidates all old details when large',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},async()=>{
 const result=localQuery(`WITH movement_source AS (
  SELECT jsonb_agg(jsonb_build_object('identity',('00000000-0000-4000-8000-'||lpad(n::text,12,'0'))::uuid,
   'candidate_id','00000000-0000-4000-8000-000000009999','row_key','movement:'||n,
   'key_type','SOURCE_REF','key_value','original-key:'||n,'from','canonical_preview_lines','to','blocked_lines','selected',false) ORDER BY n) AS all_rows
  FROM generate_series(1,5000)n
 ), samples AS (
  SELECT 'empty' AS label,'[]'::jsonb AS rows UNION ALL SELECT 'small',jsonb_build_array(all_rows->0,all_rows->1) FROM movement_source
  UNION ALL SELECT 'large',all_rows FROM movement_source
  UNION ALL SELECT 'wide',jsonb_build_array(jsonb_set(all_rows->0,'{row_key}',to_jsonb(repeat('£',5000)))) FROM movement_source
 ), actual AS (SELECT label,rows,private.pay_workbench_modal_movement_envelope_v2(rows) AS reply FROM samples)
 SELECT jsonb_build_object('label',label,'source_count',jsonb_array_length(rows),'bytes',octet_length(convert_to(reply::text,'UTF8')),
 'count',reply->'movement_count','complete',reply->'movements_complete','inline_count',jsonb_array_length(reply->'movements'),
 'inline_unchanged',reply->'movements'=rows,'hash_matches',reply->>'movement_digest'=encode(extensions.digest(convert_to(rows::text,'UTF8'),'sha256'),'hex'),
 'invalidations',reply->'invalidations','envelope',reply) FROM actual ORDER BY label;`);
 assert.equal(result.length,4);
 for(const v of result){
  assert.equal(v.count,v.source_count,v.label);assert.equal(v.hash_matches,true,v.label);
  assert.deepEqual(v.invalidations,{scope:'ALL_PREVIOUS_DETAILS',ready:true,actions:true,updating:true,blocked:true},v.label);
  assert.ok(v.bytes<=9*1024,v.label);
  if(['empty','small'].includes(v.label)){assert.equal(v.complete,true);assert.equal(v.inline_unchanged,true);}
  else {assert.equal(v.complete,false);assert.equal(v.inline_count,0);assert.ok(v.count>0);}
 }
 const worker=await import(require('node:url').pathToFileURL(path.join(__dirname,'../broker/src/banking-pay-modal-v2.js')).href);
 for(const v of result)worker.validateBankingPayMovementEnvelope({state_changed:v.count>0,...v.envelope});
 if(process.env.BANKING_MODAL_FRONTEND_ROOT){
  const frontend=require(path.join(process.env.BANKING_MODAL_FRONTEND_ROOT,'js/banking-pay-modal-v2-mutation.js'));
  for(const v of result)frontend.validateMovements({state_changed:v.count>0,...v.envelope});
 }
});
test('malformed or duplicate movement evidence cannot masquerade as an empty successful change',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const result=localQuery(`DO $negatives$
 DECLARE good jsonb:=jsonb_build_object('identity','00000000-0000-4000-8000-000000000001',
  'candidate_id','00000000-0000-4000-8000-000000000002','row_key','original-row','key_type','SOURCE_REF','key_value','original-key',
  'from','canonical_preview_lines','to','blocked_lines','selected',false);bad jsonb;k text;
 BEGIN
  FOREACH bad IN ARRAY ARRAY[NULL::jsonb,'null'::jsonb,'{}'::jsonb,'false'::jsonb,jsonb_build_array(NULL),
   jsonb_build_array(good,good),jsonb_build_array(good||'{"selected":"false"}'),
   jsonb_build_array(good||'{"to":"canonical_preview_lines"}')] LOOP
   BEGIN PERFORM private.pay_workbench_modal_movement_envelope_v2(bad);RAISE EXCEPTION 'BAD_MOVEMENT_ACCEPTED';
   EXCEPTION WHEN invalid_parameter_value THEN IF SQLERRM<>'BANKING_PAY_V2_INVALID_MOVEMENT' THEN RAISE;END IF;END;
  END LOOP;
  FOREACH k IN ARRAY ARRAY['identity','candidate_id','row_key','from','to','selected'] LOOP
   BEGIN PERFORM private.pay_workbench_modal_movement_envelope_v2(jsonb_build_array(good-k));RAISE EXCEPTION 'MISSING_MOVEMENT_FIELD_ACCEPTED';
   EXCEPTION WHEN invalid_parameter_value THEN IF SQLERRM<>'BANKING_PAY_V2_INVALID_MOVEMENT' THEN RAISE;END IF;END;
  END LOOP;
 END;$negatives$;
 SELECT jsonb_build_object('negatives','passed');`);
 assert.deepEqual(result,[{negatives:'passed'}]);
});

test('real all-page selection and recovery receipt survives the compact formatter unchanged',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL||!process.env.BANKING_MODAL_FRONTEND_ROOT},async()=>{
 const root=path.resolve(__dirname,'..');
 const setup=fs.readFileSync(path.join(__dirname,'fixtures/28082026_1429_banking_pay_selection_setup.sql'),'utf8');
 assert.ok(setup.includes("current_database()<>'banking_modal_v2_test'"));
 let sql=fs.readFileSync(path.join(__dirname,'28082026_1432_banking_pay_candidate_selection_runtime.sql'),'utf8')
  .replace('\\ir fixtures/28082026_1429_banking_pay_selection_setup.sql',()=>setup);
 assert.ok(!sql.includes('\\ir'));assert.match(sql,/ROLLBACK;\s*$/);
 sql=sql.replace(/ROLLBACK;\s*$/,`SELECT jsonb_build_object('state_changed',true)||
  private.pay_workbench_modal_movement_envelope_v2(progress_json#>'{candidate_selection_receipt_v2,result,movements}')
  FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';
  ROLLBACK;`);
 const r=require('node:child_process').spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,
  ['-X','-q','-A','-t','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
  {cwd:root,input:sql,encoding:'utf8',timeout:60000,maxBuffer:2*1024*1024});
 assert.equal(r.status,0,r.stderr||r.error?.message);
 const replies=r.stdout.trim().split(/\r?\n/).filter(Boolean).map(v=>JSON.parse(v));assert.equal(replies.length,1);
 const reply=replies[0];assert.equal(reply.movements_complete,true);assert.ok(reply.movement_count>0);
 const worker=await import(require('node:url').pathToFileURL(path.join(root,'broker/src/banking-pay-modal-v2.js')).href);
 const frontend=require(path.join(process.env.BANKING_MODAL_FRONTEND_ROOT,'js/banking-pay-modal-v2-mutation.js'));
 const candidate='10000000-0000-4000-8000-000000000002';
 worker.validateBankingPayMovementEnvelope(reply,candidate);frontend.validateMovements(reply,candidate);
});
