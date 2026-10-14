// Exact agency TEST read-only monitoring. No raw logs or credentials persisted.
import fs from 'node:fs';import {createHash} from 'node:crypto';
const sha=b=>createHash('sha256').update(b).digest('hex');
const source='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/cloudtms-backend/.codex-tmp/miget-poc.env';
const env=Object.fromEntries(fs.readFileSync(source,'utf8').split(/\r?\n/).filter(l=>l.trim()&&!l.trim().startsWith('#')&&l.includes('=')).map(l=>{const i=l.indexOf('=');return [l.slice(0,i).trim(),l.slice(i+1).trim()];}));
const token=env.MIGET_API_TOKEN;if(!token)throw Error('APPROVED_MACHINE_LOCAL_MIGET_TOKEN_UNAVAILABLE');
const headers={authorization:'Bearer '+token,accept:'application/json',...env.MIGET_WORKSPACE_ID?{'X-Workspace-Id':env.MIGET_WORKSPACE_ID}:{}};
const read=async(url)=>{const response=await fetch(url,{headers,signal:AbortSignal.timeout(20000)});if(!response.ok)throw Error('READ_ONLY_MONITOR_HTTP_'+response.status);return JSON.parse(await response.text());};
const service=await read('https://app.miget.com/api/v1/services/01a02f5a-2bee-7db2-910d-a7e71f11ba0a');
const s=service.data??service;if(s.uuid!=='01a02f5a-2bee-7db2-910d-a7e71f11ba0a'||s.name!=='postgres-service-p0qoe')throw Error('TEST_SERVICE_IDENTITY_REFUSED');
const start='2026-10-14T09:00:00Z',end='2026-10-14T09:10:00Z';
const query='{addon="postgres-service-p0qoe"} |~ "out of memory|OOM|Killed|PANIC|FATAL|terminated|restart|database system|signal"';
const log=await read('https://metrics.miget.com/loki/api/v1/query_range?'+new URLSearchParams({query,start:String(BigInt(Date.parse(start))*1000000n),end:String(BigInt(Date.parse(end))*1000000n),limit:'300',direction:'forward'}));
const events=[];for(const stream of log.data?.result??[])for(const [stamp,line]of stream.values??[]){
 const kinds=[['OUT_OF_MEMORY',/out of memory|oom/i],['KILLED_SIGNAL_9',/signal 9|killed/i],['SERVER_PROCESS_TERMINATED',/server process.*terminated/i],
  ['DATABASE_INTERRUPTED',/database system was interrupted/i],['DATABASE_READY',/database system is ready/i],['SERVER_RESTARTING',/restart/i],['PANIC',/PANIC/],['FATAL',/FATAL/]];
 const matched=kinds.filter(([,pattern])=>pattern.test(line)).map(([kind])=>kind);if(matched.length)events.push({at:new Date(Number(BigInt(stamp)/1000000n)).toISOString(),kinds:matched,
  pid:Number(line.match(/(?:PID |\[)([0-9]+)(?:\)|\])/)?.[1]??0)||null,modeAVerifierMentioned:/mode_a_verification|weekly_source_mode_a_dispatch/i.test(line),lineSha256:sha(line)});
}
const metrics=[];for(const name of ['miget_instance_memory_used_bytes','miget_instance_oom_events_total','miget_instance_restarts_total']){
 const data=await read('https://metrics.miget.com/prometheus/api/v1/query_range?'+new URLSearchParams({query:name+'{addon="postgres-service-p0qoe"}',start:String(Date.parse(start)/1000),end:String(Date.parse(end)/1000),step:'15s'}));
 for(const row of data.data?.result??[]){const values=(row.values??[]).map(([t,v])=>({at:new Date(Number(t)*1000).toISOString(),value:Number(v)}));
  metrics.push({name,addon:row.metric.addon,samples:values.length,minimum:Math.min(...values.map(x=>x.value)),maximum:Math.max(...values.map(x=>x.value)),
   transitions:values.filter((v,i)=>i&&v.value!==values[i-1].value)});}
}
const result={status:'TEST_READ_ONLY_RESTART_OBSERVATION',service:{uuid:s.uuid,name:s.name,state:s.state,quota:s.quota?{ram_size:s.quota.ram_size,cpu_size:s.quota.cpu_size}:null},
 window:{start,end},events,logLineCount:(log.data?.result??[]).reduce((n,r)=>n+(r.values?.length??0),0),metrics,
 rawLogsRetained:false,secretsReturned:false,mutations:false,checkedAt:new Date().toISOString()};
fs.writeFileSync('docs/healthroster-build/install1-rehearsal/FULL_ATTEMPT3_RESTART_OBSERVATION_14102026.json',JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result));
