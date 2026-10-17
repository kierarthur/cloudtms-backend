// Pure functions-tools companion. No Node globals/imports. Accepted COST317 is unchanged.
const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',HERE=ROOT+'/docs/healthroster-build/proposals/qt-cost-redirected-stdout-success-runtime-20261017',NODE='C:/Program Files/nodejs/node.exe';
const check=(v,c)=>{if(!v)throw Error(c);},exact=(v,keys)=>v&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).sort().join('|')===[...keys].sort().join('|'),q=s=>"'"+s.replaceAll("'","''")+"'",bytes=s=>[...s].reduce((n,c)=>n+(c.codePointAt(0)<128?1:c.codePointAt(0)<2048?2:c.codePointAt(0)<65536?3:4),0),EMPTY='e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';
export async function createRedirectedCostBridge(tools,{nonce,sourceCommit,sessionId}){
 check(/^[a-f0-9]{32}$/.test(nonce??'')&&/^[a-f0-9]{40}$/.test(sourceCommit??'')&&Number.isSafeInteger(sessionId)&&sessionId>0,'COST_BRIDGE_OWN_LAUNCH_REQUIRED');
 let cursor={offset:0,prefixSha256:EMPTY,fileId:'-'};const evidence={transport:'ACTUAL_REDIRECTED_CHILD_STDOUT',progressFile:ROOT+'/.codex-tmp/qt-cost-native-cmd-20261017-'+nonce+'/PROGRESS.jsonl',rawPtyResponses:[],progressReads:[]};
 const read=async()=>{
  const args=[nonce,sourceCommit,String(cursor.offset),cursor.prefixSha256,cursor.fileId],cmd='& '+q(NODE)+' '+q(HERE+'/reader.mjs')+' '+args.map(q).join(' ');
  let r;try{r=await tools.exec_command({cmd,workdir:ROOT,max_output_tokens:20000});}catch{throw Error('COST_BRIDGE_PUBLIC_READER_HELD');}
  check(r.exit_code===0&&!r.session_id&&typeof r.output==='string','COST_BRIDGE_PUBLIC_READER_HELD');let v;try{v=JSON.parse(r.output);}catch{throw Error('COST_BRIDGE_CLOSED_READER_REQUIRED');}
  check(exact(v,['version','nonce','offset','prefixSha256','fileId','delta'])&&v.version==='QT_COST_REDIRECTED_PROGRESS_V1'&&v.nonce===nonce&&Number.isSafeInteger(v.offset)&&v.offset>=cursor.offset&&v.offset<=16384&&/^[a-f0-9]{64}$/.test(v.prefixSha256??'')&&/^[0-9]+:[0-9]+:[0-9]+$/.test(v.fileId??'')&&(cursor.fileId==='-'||v.fileId===cursor.fileId)&&typeof v.delta==='string'&&bytes(v.delta)===v.offset-cursor.offset,'COST_BRIDGE_CLOSED_READER_REQUIRED');
  cursor={offset:v.offset,prefixSha256:v.prefixSha256,fileId:v.fileId};evidence.progressReads.push(v);return v.delta;
 };
 const initial=await read();check(initial.endsWith('\n')&&initial.split('\n').length===2,'COST_BRIDGE_EXACT_ONE_READY_PREFIX_REQUIRED');let ready;try{ready=JSON.parse(initial.slice(0,-1).replace(/\r$/,''));}catch{throw Error('COST_BRIDGE_EXACT_ONE_READY_PREFIX_REQUIRED');}
 check(exact(ready,['status','ownedDirectory','inputFile','nativeResultFile','remainingWorkMs','holderStartedAt','compatibility','businessScope','mode'])&&ready.status==='COST_INPUT_READY'&&['SCAN_PROJECT','JSON_TEXT','JSON_MD5','RECORD_BINARY','RECORD_SHA256'].includes(ready.mode)&&ready.inputFile==='INPUT.json'&&ready.nativeResultFile==='NATIVE_RESULT.json'&&ready.compatibility==='ROOT_REVIEW_REQUIRED'&&ready.businessScope==='NOT_RUN'&&Number.isSafeInteger(ready.remainingWorkMs)&&ready.remainingWorkMs>0&&ready.remainingWorkMs<=120000&&typeof ready.holderStartedAt==='string'&&Number.isFinite(Date.parse(ready.holderStartedAt))&&new Date(Date.parse(ready.holderStartedAt)).toISOString()===ready.holderStartedAt&&Date.parse(ready.holderStartedAt)<=Date.now()&&typeof ready.ownedDirectory==='string'&&new RegExp('^'+ROOT.replaceAll('.','\\.')+'/.codex-tmp/qt-audit-cost1000-20261017-[a-f0-9]{32}$').test(ready.ownedDirectory),'COST_BRIDGE_EXACT_ONE_READY_PREFIX_REQUIRED');
 // Original controller immediately applies its unchanged complete readyCheck, including original clock.
 const wrapped=Object.create(tools,{write_stdin:{enumerable:true,value:async args=>{
  check(args?.session_id===sessionId,'COST_BRIDGE_OWN_SESSION_REQUIRED');const response=await tools.write_stdin(args);evidence.rawPtyResponses.push(response);
  check(response&&typeof response==='object'&&!Array.isArray(response),'COST_BRIDGE_ACTUAL_PTY_RESPONSE_REQUIRED');const delta=await read();return {...response,output:delta};
 }}});
 return {ready,tools:wrapped,evidence};
}
