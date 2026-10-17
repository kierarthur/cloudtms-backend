// Capture complete named Health-owned functions only; no Banking body traversal.
import fs from'node:fs';import vm from'node:vm';import {get,write,sha}from'./capture.mjs';
const commit='fc302f15103102a87903bada1b61ad68dfd0cc05',file='broker/src/index.js',raw=get(commit,file),source=raw.toString(),rows=[];
for(const name of ['createClientWithSettingsAtomic','handleCreateCandidate','handleUpsertClientRate','handleTimesheetCreateManualDaily','saveAndRecalculateCanonicalDaily','runTsfinWorkerOnce','handleTimesheetAuthoriseGeneric']){
 const start=source.indexOf('async function '+name+'(');if(start<0)throw Error(name);
 let end=start,body;const re=/^\}\r?$/gm;re.lastIndex=start;
 for(const match of source.slice(start).matchAll(/^\}\r?$/gm)){end=start+match.index+match[0].length;body=source.slice(start,end);try{new vm.Script('('+body+')');break;}catch{body=null;}}
 if(!body)throw Error('complete function not found '+name);
 write('health-producers/'+name+'.js',body);rows.push({name,file,commit,startLine:source.slice(0,start).split('\n').length,bytes:Buffer.byteLength(body),sha256:sha(body),sourceOutput:'health-producers/'+name+'.js',runtimeExecuted:false});
}
write('PRODUCER_PINS.json',{commit,wholeFileBytes:raw.length,wholeFileSha256:sha(raw),functions:rows,BankBodiesRead:0,native:'NOT_RUN'});console.log(JSON.stringify(rows.map(x=>({name:x.name,bytes:x.bytes,startLine:x.startLine}))));
