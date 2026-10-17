// Controlled actual-source reproductions only. No real tool, child, native call or file output.
import test from 'node:test';import assert from 'node:assert/strict';
const A='file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/.codex-tmp/qt-audit-readonly-cost1000-source-20261017-25d4b4a24cb8b99631c33f79550a809a-closed-failure-forward/';
const{runReadyCost}=await import(new URL('root-tools.mjs',A)),{synthetic}=await import(new URL('fixtures.mjs',A)),{validateCost}=await import(new URL('validator.mjs',A));
const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',out=ROOT+'/.codex-tmp/qt-audit-cost1000-20261017-'+'b'.repeat(32);
async function reproduce(tail){
 const f=synthetic(),ready={status:'COST_INPUT_READY',ownedDirectory:out,inputFile:'INPUT.json',nativeResultFile:'NATIVE_RESULT.json',remainingWorkMs:100000,holderStartedAt:new Date(Date.now()-1000).toISOString(),compatibility:'ROOT_REVIEW_REQUIRED',businessScope:'NOT_RUN',mode:'JSON_MD5'},events=[];
 for(const row of[f.result.statements[1],f.result.statements[3],f.result.rollbackVerification.statements[0]])row.rows[0].cost_clock.observedAt=new Date(Date.now()-1000).toISOString();
 const pending=JSON.stringify({...ready,status:'COST_CAPTURED_PENDING_RELEASE'})+'\n';
 const tools={exec_command:async a=>({exit_code:0,output:JSON.stringify(a.cmd.includes("'input'")?f.built:a.cmd.includes("'result'")?{status:'COST_VALIDATED_RESULT_SAVED',capture:validateCost(f.result,f.built)}:{status:'COST_CAPTURED_OWN_TURN_RELEASED',originalSharedTurnCompleted:true})}),mcp__cloudtms_miget_operations_kier_codex__miget_db_transaction_rollback:async()=>({structuredContent:f.result}),write_stdin:async a=>{events.push(a.chars);if(a.chars==='RESULT_READY\n')return{output:pending+(tail==='DUPLICATE'?pending:tail)};if(a.chars==='RELEASE\n')return{exit_code:0,output:''};if(a.chars==='ABORT\n')return{exit_code:1,output:''};throw Error('SYNTHETIC_UNEXPECTED_POLL');}};
 const result=await runReadyCost(tools,{ready,mode:'JSON_MD5',sourceCommit:'c'.repeat(40),sessionId:123});return{result,events};
}
for(const [name,tail]of[['banner','SYNTHETIC_STARTUP_BANNER\n'],['closed-HOLD','{"status":"HOLD","code":"SYNTHETIC_HOLD"}\n'],['duplicate','DUPLICATE'],['partial-trailer','{"status":']])test('observed current source ignores '+name+' after valid pending in same tool chunk',async()=>{const r=await reproduce(tail);assert.equal(r.result.status,'BOUNDED_COST_CAPTURE_OWN_TURN_RELEASED');assert.deepEqual(r.events,['RESULT_READY\n','RELEASE\n']);});
