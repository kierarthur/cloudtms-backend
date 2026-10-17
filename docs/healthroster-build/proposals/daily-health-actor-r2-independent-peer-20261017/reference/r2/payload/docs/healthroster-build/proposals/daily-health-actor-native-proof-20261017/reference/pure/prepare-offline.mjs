// Generates clearly synthetic SQL source examples for grammar inspection only.
import fs from 'node:fs';
import path from 'node:path';
import {source,ROOT,here,sha} from './source.mjs';
import {buildInput} from './input-builder.mjs';
import {createFixedMetadataReader} from './fixed-metadata-reader.mjs';
const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:123,backendPid:100,turnId:'hr_turn_'+'a'.repeat(32),key:[20261013,1]};
const src=await source(ROOT),built=await buildInput({root:ROOT,identity});
const reader=await createFixedMetadataReader({root:ROOT,turn:{identity,signal:new AbortController().signal,assertHeld:async()=>{},runOwnedChild:async()=>{throw Error('OFFLINE_ONLY');}}});
const transaction={database:'cloudtms_test_clone',backendPid:200,xid:'50',epochZero:true},observations=[{...transaction,unit:0,rows:[]}];
const metadata=reader.inspectSource({transaction,observations,before:{backgroundRows:Object.fromEntries(src.live.backgroundRelations.map(r=>[r,[]]))}});
const value={status:'SYNTHETIC_SOURCE_EXAMPLES_NEVER_DISPATCH',syntheticIdentity:true,native:'NOT_RUN',dbConnections:0,
 programmeSha256:src.programme.sqlSha256,programmeBytes:8029,
 examples:[['ORIGINAL_PROGRAMME',src.programme.sql],['SYNTHETIC_B',built.input.sql],['SYNTHETIC_MCP_VERIFICATION',built.input.verification_sql],
  ['SYNTHETIC_C_BEFORE',metadata.input],['SYNTHETIC_C_AFTER',metadata.after.input]].map(([name,sql])=>({name,sql,sha256:sha(sql),bytes:Buffer.byteLength(sql)}))};
fs.writeFileSync(path.join(here,'OFFLINE_INPUTS.json'),JSON.stringify(value,null,2)+'\n');
console.log(JSON.stringify({...value,examples:value.examples.map(({name,sha256,bytes})=>({name,sha256,bytes}))}));
