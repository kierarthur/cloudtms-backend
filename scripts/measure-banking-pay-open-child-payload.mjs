// Synthetic, disposable-local, rollback-only measurement. Does not contact a
// Worker/provider or print/copy real payment data. No application code depends
// on this structural patch experiment.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';
import {brotliCompressSync} from 'node:zlib';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
assert.ok(process.env.BANKING_MODAL_LOCAL_PSQL,'Explicit disposable local PG17 runtime required');
const fixture=fs.readFileSync(path.join(root,'tests/28082026_1254_banking_pay_modal_ready_page_runtime.sql'),'utf8');
const marker='DO $ready_read_proof$';
assert.equal(fixture.split(marker).length,2);
const setup=fixture.slice(0,fixture.indexOf(marker));
assert.ok(setup.includes("current_database() <> 'banking_modal_v2_test'"));
const sql=setup+`
SET LOCAL client_min_messages='warning';
UPDATE public.banking_pay_workbench_session_scope SET certified_preview_publication_attestation_json=
 '{"attestation_version":"CERTIFIED_SOURCE_PREVIEW_PUBLICATION_V3","contract_version":"3","semantic_contract_version":"READY_TO_PAY_SEMANTIC_V2"}'::jsonb
WHERE session_id='00000000-0000-4000-8000-000000000005';
CREATE TEMP TABLE child_payload_measure(before_page jsonb,after_page jsonb,selection jsonb) ON COMMIT DROP;
DO $measure$
DECLARE s public.banking_pay_workbench_sessions%ROWTYPE; opts jsonb; before_page jsonb; after_page jsonb; result jsonb;
BEGIN
 SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id='00000000-0000-4000-8000-000000000005';
 opts:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
  'pay_channel_scope','ALL','scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'));
 before_page:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,'00000000-0000-4000-8000-000000000002',opts,s.actor_user_id,NULL,100);
 result:=public.pay_workbench_session_set_selected_rows(s.id,jsonb_build_object('modal_candidate_intent_v2',
  jsonb_build_object('candidate_id','00000000-0000-4000-8000-000000000002','request_id','00000000-0000-4000-8000-000000009999',
   'action','SELECT_ALL_READY','options',opts)),s.actor_user_id);
 opts:=jsonb_set(opts,'{expected_progress_counter_version}',result->'progress_counter_version');
 after_page:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,'00000000-0000-4000-8000-000000000002',opts,s.actor_user_id,NULL,100);
 INSERT INTO pg_temp.child_payload_measure VALUES(before_page,after_page,result);
END $measure$;
SELECT jsonb_build_object('before',before_page,'after',after_page,'selection',selection) FROM pg_temp.child_payload_measure;
ROLLBACK;
`;
const run=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-A','-t','-h','127.0.0.1','-p','55441','-U','postgres',
 '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{cwd:root,input:sql,encoding:'utf8',timeout:45000,maxBuffer:8*1024*1024});
assert.equal(run.status,0,run.error?.message||run.stderr);
const data=JSON.parse(run.stdout.trim());
assert.equal(data.before.rows.length,100);assert.equal(data.after.rows.length,100);
assert.equal(data.selection.progress_counter_version,data.after.progress_counter_version);
const json=value=>JSON.stringify(value);
const equal=(a,b)=>json(a)===json(b);
const object=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
function difference(before,after,pointer=''){
 if(equal(before,after))return [];
 if((object(before)&&object(after))||(Array.isArray(before)&&Array.isArray(after)&&before.length===after.length)){
  const patch=[];
  for(const key of Object.keys(before))if(!Object.hasOwn(after,key))patch.push({op:'remove',path:`${pointer}/${escape(key)}`});
  for(const [key,value] of Object.entries(after)){
   const next=`${pointer}/${escape(key)}`;
   if(!Object.hasOwn(before,key))patch.push({op:'add',path:next,value});
   else patch.push(...difference(before[key],value,next));
  }
  return patch;
 }
 return [{op:'replace',path:pointer,value:after}];
}
function escape(key){return key.replaceAll('~','~0').replaceAll('/','~1');}
const patch=difference(data.before,data.after);
function sizes(value){const encoded=Buffer.from(json(value));return {json_bytes:encoded.length,brotli_bytes:brotliCompressSync(encoded).length};}
// This deliberately reports measurements only, not a performance PASS. A body
// diff is not yet an approved, version-bound or tested application protocol.
console.log(JSON.stringify({fixture:'SYNTHETIC_105_READY_PAYMENTS',database:'disposable_loopback_55441',
 rows_in_open_page:100,complete_candidate_count:data.after.candidate.selectable_ready_count,
 before_page:sizes(data.before),after_page:sizes(data.after),internal_selection:sizes(data.selection),
 selection_with_complete_page:sizes({selection:data.selection,ready:data.after}),
 ordinary_structural_patch:sizes(patch),patch_operations:patch.length,
 selection_with_structural_patch:sizes({selection:data.selection,ready_patch:patch}),
 no_hosted_change:true,rolled_back:true}));
