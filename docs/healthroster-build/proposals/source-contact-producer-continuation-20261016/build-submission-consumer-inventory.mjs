import {execFileSync} from 'node:child_process';
import {writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(base,'../../../..');
const commit='90ffcf3f40644280fcdb5b791a62a86613896d6e';
const sha=s=>createHash('sha256').update(s).digest('hex');
const files=execFileSync('git',['grep','-l','-F','weekly_timesheet_submission_requests',commit,'--','supabase/repeatable'],
 {cwd:root,encoding:'utf8'}).trim().split('\n').map(file=>file.slice(commit.length+1));
const owners=[];
for(const file of files.filter(file=>/\.sql(inc)?$/.test(file))){
 const source=execFileSync('git',['show',`${commit}:${file}`],{cwd:root,encoding:'utf8',maxBuffer:8e6}).replace(/\r\n/g,'\n');
 if(!source.includes('weekly_timesheet_submission_requests'))continue;
 const starts=[...source.matchAll(/create or replace function ([a-z_]+\.[a-z0-9_]+)\(/g)];
 for(const start of starts){
  const bodyStart=source.indexOf('as $function$',start.index),bodyEnd=source.indexOf('$function$;',bodyStart);
  if(bodyStart<0||bodyEnd<0||starts.some(next=>next.index>start.index&&next.index<bodyStart))continue;
  const text=source.slice(start.index,bodyEnd+12);
  if(!/\b(from|join|update|into)\s+public\.weekly_timesheet_submission_requests\b/i.test(text))continue;
  const lines=text.split('\n'),lookups=[];
  for(let i=0;i<lines.length;i++)if(/\b(from|join|update|into)\s+public\.weekly_timesheet_submission_requests\b/i.test(lines[i]))
   lookups.push({source_line:source.slice(0,start.index).split('\n').length+i,excerpt:lines.slice(Math.max(0,i-1),Math.min(lines.length,i+6)).join('\n')});
  owners.push({function_name:start[1],source_path:file,source_commit:commit,function_sha256_normalized_lf:sha(text+'\n'),lookups});
 }
}
writeFileSync(path.join(base,'SUBMISSION_CONSUMER_INVENTORY.json'),JSON.stringify({version:'CONTACT_SUBMISSION_CURRENT_SOURCE_CONSUMER_INVENTORY_V1',
 source_commit:commit,owners,status:'READ_SOURCE_ONLY_NOT_CURRENT_DEFINITION_OR_NATIVE_PROOF',
 note:'Includes retained historical definitions. Root/current recipe selects exact controlling complete definitions before source successors; no Banking body is edited.'},null,2)+'\n');
console.log(JSON.stringify({source_owners:owners.length,distinct_function_names:new Set(owners.map(owner=>owner.function_name)).size}));
