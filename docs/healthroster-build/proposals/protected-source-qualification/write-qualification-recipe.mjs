import fs from 'node:fs';
import path from 'node:path';
import {exact} from '../protected-source-capability/release-qualification.mjs';
// Writes a bounded SQL include only. Never connects, executes SQL, acquires
// locks, reads credentials, invokes a provider or sends network traffic.
const [inputFile,outputFile]=process.argv.slice(2);if(!inputFile||!outputFile)throw Error('QUAL_RECIPE_TWO_PATHS_REQUIRED');
const input=JSON.parse(fs.readFileSync(inputFile,'utf8'));
exact(input,['scope_request','native_steps'],'QUAL_RECIPE_MANIFEST_INVALID');
exact(input.scope_request,['scope_id','actor_user_id','source_cycle_id','expected_execution','expected_census_sha256','capabilities','reason','turn_backend_pid'],
 'QUAL_RECIPE_SCOPE_INVALID');
if(!Array.isArray(input.native_steps)||!input.native_steps.length||input.native_steps.length>64)throw Error('QUAL_RECIPE_STEP_LIMIT');
const operations=['BULK_PREVIEW','BULK_ADMIT','BULK_ROUND','BULK_APPLY','PREPARE_FAMILY','PREPARE_ACTION','ACTION_CONTEXT',
 'STAGE_C1_REQUEST','COMPLETE_LOCAL','AUTOMATIC_PREVIEW','AUTOMATIC_COMPLETE'];
const literal=value=>"'"+String(value).replaceAll("'","''")+"'";
const rows=["-- Root-owned single rollback transaction and SEND_7 turn must already be open.",
 "-- The actual installed VERIFIED source/contract, actor and CAS are revalidated by every native step.",
 "\\set ON_ERROR_STOP on",'select private.weekly_source_protected_qualification_begin_v1('+literal(JSON.stringify(input.scope_request))+'::jsonb);'];
for(const step of input.native_steps){
 exact(step,['operation','request'],'QUAL_RECIPE_NATIVE_STEP_INVALID');if(!operations.includes(step.operation))throw Error('QUAL_RECIPE_NATIVE_STEP_INVALID');
 if(!step.request||typeof step.request!=='object'||Array.isArray(step.request))throw Error('QUAL_RECIPE_NATIVE_STEP_INVALID');
 rows.push('select private.weekly_source_protected_qualification_step_v1('+literal(input.scope_request.scope_id)+'::uuid,'+
  literal(step.operation)+','+literal(JSON.stringify(step.request))+'::jsonb);');
}
const sql=rows.join('\n')+'\n';if(Buffer.byteLength(sql)>2000000)throw Error('QUAL_RECIPE_SQL_BYTE_LIMIT');
if(rows.length-3>250)throw Error('QUAL_RECIPE_SQL_STATEMENT_LIMIT');
fs.mkdirSync(path.dirname(path.resolve(outputFile)),{recursive:true});fs.writeFileSync(outputFile,sql);
console.log(JSON.stringify({sql_bytes:Buffer.byteLength(sql),statements:input.native_steps.length+1,executed:false}));
