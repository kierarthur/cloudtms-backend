// Actual unchanged Health V3 decoder; fixtures prove projection/transport only.
import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL,fileURLToPath} from 'node:url';
import assert from 'node:assert/strict';
const here="D:\\Development\\GitHub\\.codex-worktrees\\healthroster-v2-money-build-13102026\\docs\\healthroster-build\\proposals\\contract-preview-current-independent-peer-20261017\\replay";
const root="D:\\Development\\GitHub\\.codex-worktrees\\healthroster-v2-money-build-13102026\\docs\\healthroster-build\\proposals\\contract-preview-current-independent-peer-20261017\\replay\\current";
const contract=await import(pathToFileURL(path.join(root,'broker/src/contract-correction-v3.mjs')));
const fixtures=JSON.parse(fs.readFileSync(path.join(here,'AST_PROJECTION_VECTORS.json'),'utf8'));
let checks=0;const pass=fn=>{fn();checks++;},refuse=fn=>pass(()=>assert.throws(fn));
for(const row of fixtures.invoice)pass(()=>contract.validateEffect(row.effect));
for(const row of fixtures.results){
 const options={financialVisible:row.visible,billingVisible:row.visible,retainedNativePending:row.retainedNativePending};
 pass(()=>contract.validateResult(row.result,options));
 for(const mutate of [value=>value.state_revision=9007199254740995,value=>value.state_revision='0',value=>value.raw_receipt={},
  value=>value.financial.native_snapshot={},value=>value.placement.placement_revision='092',value=>value.billing.revision='9223372036854775808']){
  const bad=structuredClone(row.result);mutate(bad);refuse(()=>contract.validateResult(bad,options));
 }
 if(!row.visible){for(const field of ['command_id','cut_id']){const bad=structuredClone(row.result);bad.financial[field]='10000000-0000-0000-0000-000000000041';refuse(()=>contract.validateResult(bad,options));}
  const bad=structuredClone(row.result);bad.billing.visible_summary={visibility:'FULL',text:'Denied data'};refuse(()=>contract.validateResult(bad,options));}
 if(row.retainedNativePending)refuse(()=>contract.validateResult(row.result));
 if(row.result.state==='COMPLETE'){const bad=structuredClone(row.result);bad.placement.status='NOT_COMMITTED';bad.placement.receipt_id=null;refuse(()=>contract.validateResult(bad,options));}
}
const id=n=>`10000000-0000-0000-0000-${String(n).padStart(12,'0')}`;
const request={version:'TIMESHEET_CONTRACT_CORRECTION_V3',request_id:id(1),timesheet_id:id(2),to_contract_id:id(3)};
pass(()=>contract.validatePreviewRequest(request));
pass(()=>contract.validatePreviewRequest({...request,actor_user_id:id(4)},{service:true}));
for(const extra of [{actor_user_id:id(4)},{agency_id:id(5)},{rate:'10'},{financial_command_id:id(5)},{cause:'correcting'}])
 refuse(()=>contract.validatePreviewRequest({...request,...extra}));
refuse(()=>contract.validatePreviewRequest({...request,reason:null}));
refuse(()=>contract.validatePreviewRequest({...request,to_contract_id:null}));
pass(()=>assert.equal(contract.normalizeReason('\u00a0\ufeff'), 'Correcting the Contract'));
pass(()=>assert.equal(contract.normalizeReason('\u00a0\ufeffNote\u202f'), 'Correcting the Contract — Note'));
pass(()=>assert.equal(contract.normalizeReason('😀'.repeat(1969)), 'Correcting the Contract — '+'😀'.repeat(1969)));
refuse(()=>contract.normalizeReason('😀'.repeat(1970)));
const accept={version:request.version,request_id:id(6),operation_id:id(7),review_id:id(8),review_digest:'a'.repeat(64),expected_state_revision:'9223372036854775807'};
pass(()=>contract.validateAcceptRequest(accept));
for(const counter of [0,1,9007199254740993,'0','01','9223372036854775808'])refuse(()=>contract.validateAcceptRequest({...accept,expected_state_revision:counter}));
refuse(()=>contract.validateAcceptRequest({...accept,to_contract_id:id(3)}));
const read={version:request.version,action:'READ_RESULT',operation_id:id(7),request_id:id(6),actor_user_id:id(4)};
pass(()=>contract.validateReadRequest(read));
for(const extra of [{reason:'new reason'},{action:'ACCEPT'},{expected_state_revision:'1'},{rate:'0'},{request_id:null}])
 refuse(()=>contract.validateReadRequest({...read,...extra}));
const redacted={visibility:'REDACTED'};
refuse(()=>contract.validateEffect({...redacted,text:'Hidden'}));
refuse(()=>contract.validateEffect({...redacted,amount:'0'}));
console.log(JSON.stringify({assertions:checks,source:'ACTUAL_UNCHANGED_ROOT_V3_DECODER',projectionASTFixtures:fixtures.results.length+fixtures.invoice.length,nativeAuthorityProved:false}));
