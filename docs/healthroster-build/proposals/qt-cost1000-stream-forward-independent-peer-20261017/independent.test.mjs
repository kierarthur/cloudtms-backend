// Public source imports and in-memory callbacks only. No provider, child, file output or source admission.
import test from 'node:test';
import assert from 'node:assert/strict';
const AUTHOR='file:///D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/.codex-tmp/qt-audit-readonly-cost1000-source-20261017-25d4b4a24cb8b99631c33f79550a809a-closed-failure-forward-stream-forward/';
const {runReadyCost}=await import(new URL('root-tools.mjs',AUTHOR));
const {synthetic}=await import(new URL('fixtures.mjs',AUTHOR));
const {validateCost}=await import(new URL('validator.mjs',AUTHOR));
const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const out=ROOT+'/.codex-tmp/qt-audit-cost1000-20261017-'+'d'.repeat(32);
function fixture({pendingTail='',pendingPrefix='',endTail='',splitEnd=false,projection={status:'COST_CAPTURED_OWN_TURN_RELEASED',originalSharedTurnCompleted:true},summaryPatch={}}={}){
 const f=synthetic(),events=[];
 const ready={status:'COST_INPUT_READY',ownedDirectory:out,inputFile:'INPUT.json',nativeResultFile:'NATIVE_RESULT.json',remainingWorkMs:100000,holderStartedAt:new Date(Date.now()-1000).toISOString(),compatibility:'ROOT_REVIEW_REQUIRED',businessScope:'NOT_RUN',mode:'JSON_MD5'};
 for(const row of [f.result.statements[1],f.result.statements[3],f.result.rollbackVerification.statements[0]])row.rows[0].cost_clock.observedAt=new Date(Date.now()-1000).toISOString();
 const pending=JSON.stringify({...ready,status:'COST_CAPTURED_PENDING_RELEASE'})+'\n';
 const summary=JSON.stringify({status:'COST_CAPTURED_OWN_TURN_RELEASED',ownedDirectory:out,businessScope:'NOT_RUN',compatibility:'ROOT_REVIEW_REQUIRED_NOT_QUALIFIED',...summaryPatch})+'\n';
 let endQueue=[],aborted=false,released=false;
 const tools={
  exec_command:async a=>{events.push(['io',a.cmd]);return {exit_code:0,output:JSON.stringify(a.cmd.includes("'input'")?f.built:a.cmd.includes("'result'")?{status:'COST_VALIDATED_RESULT_SAVED',capture:validateCost(f.result,f.built)}:projection)};},
  mcp__cloudtms_miget_operations_kier_codex__miget_db_transaction_rollback:async()=>{events.push(['native']);return {structuredContent:f.result};},
  write_stdin:async a=>{
   events.push(['control',a.chars]);
   if(a.chars==='ABORT\n'){aborted=true;return {exit_code:1,output:''};}
   if(aborted)return {exit_code:1,output:''};
   if(a.chars==='RESULT_READY\n')return {output:pendingPrefix+pending+(pendingTail==='DUPLICATE'?pending:pendingTail)};
   if(a.chars==='RELEASE\n'){
    released=true;
    if(splitEnd){endQueue=[{output:summary.slice(12)+endTail},{exit_code:0,output:''}];return {output:summary.slice(0,12)};}
    if(endTail){endQueue=[{exit_code:0,output:endTail}];return {output:summary};}
    return {exit_code:0,output:summary};
   }
   if(a.chars===''&&released&&endQueue.length)return endQueue.shift();
   throw Error('SYNTHETIC_UNEXPECTED_POLL');
  }
 };
 return {events,tools,args:{ready,mode:'JSON_MD5',sourceCommit:'e'.repeat(40),sessionId:789}};
}
const controls=x=>x.events.filter(e=>e[0]==='control'&&e[1]).map(e=>e[1]);
for(const [name,pendingTail]of [['banner','SYNTHETIC_BANNER\n'],['HOLD','{"status":"HOLD"}\n'],['duplicate','DUPLICATE'],['partial','{"status":']])test('actual corrected full function refuses original '+name+' before RELEASE',async()=>{
 const x=fixture({pendingTail});await assert.rejects(runReadyCost(x.tools,x.args));assert.deepEqual(controls(x),['RESULT_READY\n','ABORT\n']);assert.equal(x.events.filter(e=>e[0]==='native').length,1);
});
test('unknown complete record preceding valid pending also refuses before RELEASE',async()=>{
 const x=fixture({pendingPrefix:'{"status":"HOLD"}\n'});await assert.rejects(runReadyCost(x.tools,x.args));assert.deepEqual(controls(x),['RESULT_READY\n','ABORT\n']);
});
test('valid summary may span polls, but no summary-file proof is read before actual exit0',async()=>{
 const x=fixture({splitEnd:true});const r=await runReadyCost(x.tools,x.args);assert.equal(r.status,'BOUNDED_COST_CAPTURE_OWN_TURN_RELEASED');assert.deepEqual(controls(x),['RESULT_READY\n','RELEASE\n']);const lastPoll=x.events.findLastIndex(e=>e[0]==='control');const proof=x.events.findIndex(e=>e[0]==='io'&&e[1].includes("'summary'"));assert(proof>lastPoll);
});
for(const [name,endTail]of [['later-HOLD','{"status":"HOLD"}\n'],['later-partial','{"status":']])test('valid summary followed by '+name+' in a later poll cannot pass',async()=>{
 const x=fixture({endTail});await assert.rejects(runReadyCost(x.tools,x.args));assert(x.events.some(e=>e[0]==='control'&&e[1]==='RELEASE\n'));assert(!x.events.some(e=>e[0]==='io'&&e[1].includes("'summary'")));
});
test('a summary cannot promote a business/compatibility status',async()=>{
 for(const summaryPatch of [{businessScope:'PASS'},{compatibility:'QUALIFIED'},{unexpected:true}]){const x=fixture({summaryPatch});await assert.rejects(runReadyCost(x.tools,x.args));assert(!x.events.some(e=>e[0]==='io'&&e[1].includes("'summary'")));}
});
test('normal stdout and exit cannot replace exact original completion projection',async()=>{
 for(const projection of [{status:'COST_CAPTURED_OWN_TURN_RELEASED'},{status:'COST_CAPTURED_OWN_TURN_RELEASED',originalSharedTurnCompleted:false}]){const x=fixture({projection});await assert.rejects(runReadyCost(x.tools,x.args));}
});
