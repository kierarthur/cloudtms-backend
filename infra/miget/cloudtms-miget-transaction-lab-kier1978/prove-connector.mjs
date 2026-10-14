import fs from 'node:fs';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {withSharedRehearsalTurn} from '../../../scripts/healthroster-rehearsal-turn.mjs';

const endpoint='https://codex-kier1978-miget-transaction-lab.kier-88a.workers.dev/mcp';
const token=process.env.CLOUDTMS_MIGET_TRANSACTION_LAB_KIER1978_TOKEN;
if(!token)throw Error('OWN_ACCOUNT_CREDENTIAL_REQUIRED');
let id=0,session;
async function rpc(method,params,notification=false){
 const response=await fetch(endpoint,{method:'POST',headers:{authorization:'Bearer '+token,
  'content-type':'application/json',accept:'application/json, text/event-stream',
  'MCP-Protocol-Version':'2025-06-18',...(session?{'Mcp-Session-Id':session}:{})},
  body:JSON.stringify({jsonrpc:'2.0',...notification?{}:{id:++id},method,params}),signal:AbortSignal.timeout(120000)});
 session=response.headers.get('mcp-session-id')??session;
 assert.ok(response.ok,'AUTHENTICATED_MCP_HTTP_FAILED');
 const source=await response.text();if(notification)return {status:response.status};
 const envelopes=source.trim().startsWith('{')?[JSON.parse(source)]:source.split(/\r?\n/).filter(x=>x.startsWith('data: ')).map(x=>JSON.parse(x.slice(6)));
 const frame=envelopes.find(x=>x.id===id);assert.ok(frame&&!frame.error,'MCP_RESPONSE_ENVELOPE_FAILED');return frame.result;
}
const receipt={account:'kierarthur1978@gmail.com',endpoint,checkedAt:new Date().toISOString(),
 status:'PREPARING',nativeCodexToolPickup:'RESTART_AND_NEW_TASK_REQUIRED',secretsReturned:false,
 sourceCommit:'6829ffbc7d5323c9198443e652e6d4b7f03adf8f',sharedTurnEvents:[]};
const save=()=>fs.writeFileSync(new URL('./ACCEPTANCE.json',import.meta.url),JSON.stringify(receipt,null,2)+'\n');
save();
try{
 receipt.anonymousStatus=(await fetch(endpoint,{signal:AbortSignal.timeout(15000)})).status;
 assert.equal(receipt.anonymousStatus,401);
 receipt.operationsAnonymousStatus=(await fetch('https://codex-kier1978-miget-operations.kier-88a.workers.dev/mcp',{signal:AbortSignal.timeout(15000)})).status;
 assert.equal(receipt.operationsAnonymousStatus,401);
 receipt.initialize=await rpc('initialize',{protocolVersion:'2025-06-18',capabilities:{},clientInfo:{name:'kier1978-account-acceptance',version:'1.0.0'}});
 await rpc('notifications/initialized',{},true);
 const catalogue=await rpc('tools/list',{});
 assert.deepEqual(catalogue.tools.map(x=>x.name),['miget_db_transaction_rollback']);
 receipt.toolNames=catalogue.tools.map(x=>x.name);
 const marker='kier1978_rollback_probe_'+crypto.randomBytes(12).toString('hex');
 const routine=marker+'_fn';
 const sql=`create table private.${marker}(id integer primary key,value text not null);
 insert into private.${marker} values(1,'temporary');
 create function private.${routine}() returns text language sql as $$select 'temporary'::text$$;
 select current_database() as database,(select count(*)::integer from private.${marker}) as temporary_rows,private.${routine}() as temporary_function;`;
 const verification_sql=`select current_database() as database,to_regclass('private.${marker}') is null as table_absent,to_regprocedure('private.${routine}()') is null as function_absent;`;
 receipt.proofSqlSha256=crypto.createHash('sha256').update(sql).digest('hex');
 receipt.proofSql=sql;receipt.verificationSql=verification_sql;
 const result=await withSharedRehearsalTurn({task:'Kier1978 dedicated connector installation rollback acceptance',
  progressPath:new URL('./SETUP_PROGRESS.md',import.meta.url).pathname,
  async work(turn){
   receipt.turnIdentity=turn.identity;
   const before=await turn.turnConnection.query(`select to_regclass('private.${marker}') is null and to_regprocedure('private.${routine}()') is null;`);
   assert.deepEqual(before,['t']);await turn.assertHeld('before-new-account-rollback-proof');
   const envelope=await rpc('tools/call',{name:'miget_db_transaction_rollback',arguments:{label:'Kier1978 isolated connector acceptance',sql,verification_sql,statement_timeout_ms:10000,lock_timeout_ms:2000,max_rows_per_statement:5,continue_on_error:false}});
   assert.ok(!envelope.isError,'LAB_TOOL_FAILED');
   const actual=envelope.structuredContent??JSON.parse(envelope.content.find(x=>x.type==='text').text);
   receipt.rollbackResult=actual;save();
   assert.equal(actual.accepted,true);assert.equal(actual.database,'cloudtms_test_clone');assert.equal(actual.rolledBack,true);
   assert.equal(actual.rollbackVerification.connectedFresh,true);assert.equal(actual.rollbackVerification.database,'cloudtms_test_clone');
   assert.equal(actual.sqlSha256,receipt.proofSqlSha256);assert.equal(actual.statementCount,4);
   assert.ok(actual.statements.every(x=>!x.error));
   assert.deepEqual(actual.statements[3].rows,[{database:'cloudtms_test_clone',temporary_rows:1,temporary_function:'temporary'}]);
   assert.deepEqual(actual.rollbackVerification.statements[0].rows,[{database:'cloudtms_test_clone',table_absent:true,function_absent:true}]);
   await turn.assertHeld('after-new-account-fresh-absence');return {status:'ROLLBACK_AND_FRESH_ABSENCE_PASS'};
  }},{log(event){receipt.sharedTurnEvents.push(event);save();}});
 assert.equal(result.status,'ROLLBACK_AND_FRESH_ABSENCE_PASS');
 const closed=receipt.sharedTurnEvents.at(-1);
 assert.equal(closed.status,'RELEASED');assert.equal(closed.explicitUnlockConfirmed,true);assert.equal(closed.connectionClosed,true);assert.equal(closed.proved,true);
 const refused=await rpc('tools/call',{name:'miget_db_transaction_rollback',arguments:{label:'Kier1978 transaction escape refusal',sql:'commit;'}});
 assert.equal(refused.isError,true,'OUTER_TRANSACTION_ESCAPE_MUST_BE_REFUSED');
 receipt.outerTransactionEscapeRefused=true;receipt.status='HOSTED_ROLLBACK_CONNECTOR_ACCEPTANCE_PASS';
}catch{receipt.status='HELD_NOT_COMPLETE';process.exitCode=1;}
finally{receipt.completedAt=new Date().toISOString();save();console.log(JSON.stringify({status:receipt.status,anonymousStatus:receipt.anonymousStatus,
 rolledBack:receipt.rollbackResult?.rolledBack,connectedFresh:receipt.rollbackResult?.rollbackVerification?.connectedFresh,
 sharedLockReleased:receipt.sharedTurnEvents.at(-1)?.explicitUnlockConfirmed,nativeCodexToolPickup:receipt.nativeCodexToolPickup}));}
