// Approved machine-local token, exact TEST service, read-only narrow Loki window.
import fs from 'node:fs';import {createHash} from 'node:crypto';
const sha=b=>createHash('sha256').update(b).digest('hex');
const source='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/cloudtms-backend/.codex-tmp/miget-poc.env';
const env=Object.fromEntries(fs.readFileSync(source,'utf8').split(/\r?\n/).filter(l=>l.trim()&&!l.trim().startsWith('#')&&l.includes('=')).map(l=>{const i=l.indexOf('=');return [l.slice(0,i).trim(),l.slice(i+1).trim()];}));
if(!env.MIGET_API_TOKEN)throw Error('APPROVED_MACHINE_LOCAL_TOKEN_REQUIRED');
const headers={authorization:'Bearer '+env.MIGET_API_TOKEN,accept:'application/json',...env.MIGET_WORKSPACE_ID?{'X-Workspace-Id':env.MIGET_WORKSPACE_ID}:{}};
const read=async url=>{const r=await fetch(url,{headers,signal:AbortSignal.timeout(20000)});if(!r.ok)throw Error('READ_ONLY_LOG_HTTP_'+r.status);return r.json();};
const service=await read('https://app.miget.com/api/v1/services/01a02f5a-2bee-7db2-910d-a7e71f11ba0a'),s=service.data??service;
if(s.uuid!=='01a02f5a-2bee-7db2-910d-a7e71f11ba0a'||s.name!=='postgres-service-p0qoe')throw Error('EXACT_TEST_SERVICE_REFUSED');
const start='2026-10-14T09:07:20Z',end='2026-10-14T09:08:30Z';
const queries=['{app="postgres-service-p0qoe"}','{app="cloudtms-codex-poc-pg17"}','{addon="postgres-service-p0qoe"}'];
const results=[];
for(const query of queries){const log=await read('https://metrics.miget.com/loki/api/v1/query_range?'+new URLSearchParams({query,start:String(BigInt(Date.parse(start))*1000000n),end:String(BigInt(Date.parse(end))*1000000n),limit:'200',direction:'forward'}));
 const events=[];let lines=0;
 for(const stream of log.data?.result??[])for(const [stamp,line]of stream.values??[]){lines++;
  const kinds=[['OUT_OF_MEMORY',/out of memory|\boom\b/i],['KILLED_SIGNAL_9',/signal 9|killed/i],['SERVER_PROCESS_TERMINATED',/server process.*terminated/i],['DATABASE_INTERRUPTED',/database system was interrupted/i],['DATABASE_READY',/database system is ready/i],['SERVER_RESTARTING',/restart/i],['PANIC',/\bPANIC\b/],['FATAL',/\bFATAL\b/],['SHUTDOWN',/shutdown request|shutting down/i],['CRASH_RECOVERY',/recovery|redo starts|redo done/i]];
  const matched=kinds.filter(([,re])=>re.test(line)).map(([kind])=>kind);if(matched.length)events.push({at:new Date(Number(BigInt(stamp)/1000000n)).toISOString(),kinds:matched,lineSha256:sha(line),modeAVerifierMentioned:/mode_a_verification|weekly_source_mode_a_dispatch/i.test(line)});
 }
 results.push({selector:query,lineCount:lines,streamCount:log.data?.result?.length??0,events,limited:lines===200});
}
const result={status:'EXACT_TEST_READ_ONLY_LOG_LABEL_OBSERVATION',service:{uuid:s.uuid,name:s.name},window:{start,end},results,rawLogsRetained:false,secretsReturned:false,mutations:false,checkedAt:new Date().toISOString()};
fs.writeFileSync('docs/healthroster-build/install1-rehearsal/FULL_ATTEMPT3_RESTART_LOG_OBSERVATION_14102026.json',JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result));
