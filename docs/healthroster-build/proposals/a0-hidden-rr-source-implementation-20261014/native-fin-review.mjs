// Published I2 source only, no database evaluation.
import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
import {sqlTokens} from '../cn-a0-isolation-source-followup/sql-function-source-index.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),closure=JSON.parse(fs.readFileSync(path.join(here,'ACTUAL_SOURCE_CALLEE_AND_TRIGGER_CLOSURE.json')));
function writes(body){const t=sqlTokens(body),out=[];for(let i=0;i<t.length;i++){if(t[i].kind!=='word')continue;let op,j;if(t[i].value==='insert'&&t[i+1]?.value==='into'){op='INSERT';j=i+2;}else if(t[i].value==='update'){op='UPDATE';j=i+1;}else if(t[i].value==='delete'&&t[i+1]?.value==='from'){op='DELETE';j=i+2;}else continue;if(t[j]?.value==='only')j++;if(t[j]?.value==='public'&&t[j+1]?.value==='.')j+=2;if(t[j]?.value==='timesheets_financials')out.push({operation:op,bodyLine:body.slice(0,t[i].start).split('\n').length});}return out;}
const writers=closure.routines.filter(p=>p.body!==undefined).map(p=>({...p,mutations:writes(p.body)})).filter(p=>p.mutations.length),nativeTriggers=JSON.parse(fs.readFileSync(process.argv[2])).triggers.filter(t=>t.table==='timesheets_financials');
fs.writeFileSync(path.join(here,'ACTUAL_NATIVE_FIN_WRITER_PAGES.json'),JSON.stringify({sourceOnly:true,rootCommit:closure.rootCommit,nativeRun:false,writers,nativeTriggers},null,2)+'\n');
console.log(JSON.stringify({writers:writers.map(p=>({id:p.id,file:p.file,mutations:p.mutations,handlers:p.exceptions.map(e=>e.bodyLine)})),nativeTriggers},null,2));
