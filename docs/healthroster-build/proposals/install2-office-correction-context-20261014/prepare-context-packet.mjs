import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),prior=path.dirname(dir),draft=path.join(prior,'install2-office-consumer-forward-20261014');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');if(fs.existsSync(path.join(dir,'REVIEW_MANIFEST.json')))throw Error('Preserve freeze');
for(const p of ['BACKEND_CONTEXT_FULL_REVERSE.json','correction-context.test.mjs','prepare-correction-context-forward.mjs',
 'forward/backend/broker/src/index.js','forward/backend/broker/src/timesheet-correction-office-context.mjs',
 'reference/backend/broker/src/timesheet-correction-office-context.mjs','reference/backend/broker/src/timesheet-correction-office-routes.mjs',
 'reference/backend/broker/src/payment-actor-permission.js']){const to=path.join(dir,p);fs.mkdirSync(path.dirname(to),{recursive:true});fs.copyFileSync(path.join(draft,p),to);}
const current=path.join(prior,'install2-a0-scoped-pay-permission-successor-20261014');const inputs=[];
for(const p of ['REVIEW_MANIFEST.json','SOURCE_CHECK_RESULT.json','forward/broker/src/index.js','forward/broker/src/timesheet-correction-office-routes.mjs'])inputs.push({path:path.relative(prior,path.join(current,p)).replaceAll('\\','/'),sha256:sha(fs.readFileSync(path.join(current,p)))});
fs.writeFileSync(path.join(dir,'SOURCE_INPUTS.json'),JSON.stringify({source_only:true,inputs,public_read_rpc_signatures:[
 'public.timesheet_correction_create_projection_v1(uuid,uuid,uuid,uuid)','public.timesheet_correction_request_projection_v1(uuid,uuid)',
 'public.timesheet_correction_request_authority_v1(uuid,uuid)'],native_owners_pin_source:'../install2-a0-runtime-route-wiring-20261014/NATIVE_OWNER_SOURCE_PINS.json',new_sql_signatures:0,native_calls:0},null,2)+'\n');
