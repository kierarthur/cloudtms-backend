import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),file=path.join(out,'INTEGRATION_ORDER.json'),integration=JSON.parse(fs.readFileSync(file));
const sql=integration.sql_complete_successors_in_final_order;
for(const name of ['13_complete_nullable_physical_qualification_scope.sqlinc','OFF_RULE_NATIVE_PARENT_SCOPE.sqlinc'])if(!sql.includes(name))sql.splice(sql.indexOf('OFF_RULE_COMMAND.sqlinc'),0,name);
integration.acl_final_gate=integration.acl_final_gate.replace('interface25','interface26');
integration.critic_corrections='Command takes nonwaiting same scope gate before OPEN revalidation and retains it through native completion; unified owner-only resolver admits every declared native parent before audit frame. Actual orchestration cycle comes from complete authenticated prepare trace plus actual native request fingerprint, or real native approval; no invented run column. Captured physical NULL Client roots are rechecked during Automatic scope/CLOSE and bulk. Original Source39/items unchanged. Shared loader preserves unchanged no-descriptor caller, WITH-context raw rows, qualifications[] and optional Report artifacts.';
fs.writeFileSync(file,JSON.stringify(integration,null,2)+'\n');
const memo=path.join(out,'SOURCE_REVIEW_READY.md');let text=fs.readFileSync(memo,'utf8');
text=text.replace('closure of 487 native/triggered routines','closure of 488 native/triggered routines (including the actual publication-status owner omitted from the previous census)')
 .replace('with 25 interface owner-only additions: 300','with 26 interface owner-only additions: 301')
 .replace('Sixteen focused tests cover','Twenty-one focused tests cover');fs.writeFileSync(memo,text);
