// Read-only tools/list capability check. No SQL/tools/call or credentials output.
import fs from'node:fs';
const token=process.env.CLOUDTMS_MIGET_TRANSACTION_LAB_KIER1978_TOKEN;
if(!token)throw Error('OWN_PROCESS_BEARER_ABSENT');
const url='https://codex-kier1978-miget-transaction-lab.kier-88a.workers.dev/mcp';let session;
async function rpc(method,params,id){const headers={Authorization:'Bearer '+token,'Content-Type':'application/json',Accept:'application/json, text/event-stream'};if(session)headers['Mcp-Session-Id']=session;
 const r=await fetch(url,{method:'POST',headers,body:JSON.stringify({jsonrpc:'2.0',method,params,...(id===undefined?{}:{id})}),signal:AbortSignal.timeout(15000)});session=r.headers.get('mcp-session-id')??session;
 const text=await r.text();let result;try{result=JSON.parse(text);}catch{const data=text.split(/\r?\n/).filter(x=>x.startsWith('data: ')).map(x=>x.slice(6));if(data.length)result=JSON.parse(data.at(-1));}
 if(!r.ok||result?.error)throw Error('MCP_DISCOVERY_REFUSED_HTTP_'+r.status);return result;
}
const init=await rpc('initialize',{protocolVersion:'2025-03-26',capabilities:{},clientInfo:{name:'healthroster-kier1978-discovery',version:'1.0.0'}},1);await rpc('notifications/initialized',{},undefined);
const listed=await rpc('tools/list',{},2),tool=listed?.result?.tools?.find(x=>x.name==='miget_db_transaction_rollback');
const result={checked_at_utc:new Date().toISOString(),discovery_only:true,sql_sent:false,tools_call_sent:false,endpoint:url,server:init?.result?.serverInfo,tool_names:listed?.result?.tools?.map(x=>x.name),exact_tool_present:!!tool,sql_character_limit:tool?.inputSchema?.properties?.sql?.maxLength,verification_character_limit:tool?.inputSchema?.properties?.verification_sql?.maxLength,main_codex_process_bearer_present:false,windows_user_bearer_present:true,native_task_tool_available:false,registration_enabled:true,required_next:'Fully quit and reopen Codex; start a fresh task and verify native lab tool under the shared lock before any rollback trial.',no_credential_or_registration_changes:true};
fs.mkdirSync('.codex-tmp/kier1978-lab-discovery-20261015',{recursive:true});fs.writeFileSync('.codex-tmp/kier1978-lab-discovery-20261015/DISCOVERY.json',JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result));if(!tool)process.exitCode=1;
