// Retains immutable author vectors and creates transparent actual-root import/read bindings.
import fs from 'node:fs';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';
const base='docs/healthroster-build/install2-composition/',packet='docs/healthroster-build/proposals/',sha=b=>createHash('sha256').update(b).digest('hex'),records=[];
const jobs=[
 ['install2-a0-scoped-pay-permission-successor-20261014/scoped-pay-successor.test.mjs','scoped-correction-current-pay.test.mjs',[
  ['./forward/broker/src/timesheet-correction-orchestrator.mjs','../../../broker/src/timesheet-correction-orchestrator.mjs'],
  ['./forward/broker/src/timesheet-correction-office-routes.mjs','../../../broker/src/timesheet-correction-office-routes.mjs'],
  ['./reference/broker/src/weekly-source/authorise-routing.mjs','../../../broker/src/weekly-source/authorise-routing.mjs'],
  ['./reference/broker/src/payment-actor-permission.js','../../../broker/src/payment-actor-permission.js'],
  ['./forward/broker/src/index.js','../../../broker/src/index.js'],
  ['./CURRENT_NATIVE_SCOPED_ACTOR_OWNER.js.txt','../proposals/install2-a0-scoped-pay-permission-successor-20261014/CURRENT_NATIVE_SCOPED_ACTOR_OWNER.js.txt'],
  ['./CURRENT_SCOPED_PAY_FULL_REVERSE.json','../proposals/install2-a0-scoped-pay-permission-successor-20261014/CURRENT_SCOPED_PAY_FULL_REVERSE.json'],
  ["const actualAuth =", "assert.equal(source.split(scopedOwner).length, 2, 'Actual root scoped actor owner must equal the frozen complete snippet');\nconst actualAuth ="]
 ]],
 ['install2-office-correction-context-20261014/correction-context.test.mjs','scoped-correction-current-context.test.mjs',[
  ['./reference/backend/broker/src/timesheet-correction-office-context.mjs','../../../broker/src/timesheet-correction-office-context.mjs'],
  ['../install2-a0-scoped-pay-permission-successor-20261014/reference/broker/src/weekly-source/authorise-routing.mjs','../../../broker/src/weekly-source/authorise-routing.mjs'],
  ['./BACKEND_CONTEXT_FULL_REVERSE.json','../proposals/install2-office-correction-context-20261014/BACKEND_CONTEXT_FULL_REVERSE.json'],
  ["const env=", "const actualIndex=fs.readFileSync(new URL('../../../broker/src/index.js',import.meta.url),'utf8');\nconst env="],
  ['h=reverse.hunks[1].replacement;','h=reverse.hunks[1].replacement;assert.equal(actualIndex.split(h).length,2,\'Actual root context owner must equal the frozen full hunk\');']
 ]]
];
for(const [origin,destination,specific]of jobs){const original=fs.readFileSync(packet+origin),changes=[...specific,['../install2-a0-runtime-route-wiring-20261014/unit-fixtures.mjs','../proposals/install2-a0-runtime-route-wiring-20261014/unit-fixtures.mjs'],['../install2-a0-auth-tenant-successor-20261014/CURRENT_AUTH_OWNER_SOURCE.json','../proposals/install2-a0-auth-tenant-successor-20261014/CURRENT_AUTH_OWNER_SOURCE.json']];let text=original.toString();for(const [before,after]of changes){assert.equal(text.split(before).length,2,'BINDING_NOT_UNIQUE '+before);text=text.replace(before,()=>after);}const output=Buffer.from(text);assert.ok(!fs.existsSync(base+destination));fs.writeFileSync(base+destination,output);records.push({source:packet+origin,sourceSha256:sha(original),destination:base+destination,destinationSha256:sha(output),exactSubstitutions:changes});}
fs.writeFileSync(base+'SCOPED_CORRECTION_ACTUAL_ROOT_TEST_BINDINGS_14102026.json',JSON.stringify({sourceOnly:true,records,unitOnly:true,noDbNetworkOrNativeCall:true},null,2)+'\n');
