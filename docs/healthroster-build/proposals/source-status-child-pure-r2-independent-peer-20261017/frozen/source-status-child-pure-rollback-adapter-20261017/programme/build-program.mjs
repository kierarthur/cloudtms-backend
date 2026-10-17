// Pure source construction only. No CLI, database client, callback or file sink.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';

const here=path.dirname(fileURLToPath(import.meta.url));
const manifest=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json'),'utf8'));
const sha=bytes=>crypto.createHash('sha256').update(bytes).digest('hex');
const check=(ok,code)=>{if(!ok)throw Error(code);};

function extract(bytes,name){
 const text=bytes.toString('utf8'),prefix='create or replace function '+name+'(';
 const start=text.indexOf(prefix);
 check(start>=0&&text.indexOf(prefix,start+1)===-1,'SOURCE_PURE_DEFINITION_UNIQUE_REQUIRED');
 const body=text.indexOf('as $function$',start);
 const end=text.indexOf('$function$;',body+'as $function$'.length);
 check(body>start&&end>body,'SOURCE_PURE_DEFINITION_BOUNDARY_REQUIRED');
 return Buffer.from(text.slice(start,end+'$function$;'.length));
}

export function buildPureProgram({root}){
 check(typeof root==='string'&&path.isAbsolute(root),'SOURCE_PURE_ABSOLUTE_ROOT_REQUIRED');
 const physical=fs.realpathSync.native(root).replaceAll('\\','/');
 check(physical==='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',
  'SOURCE_PURE_REGISTERED_ROOT_REQUIRED');
 for(const input of manifest.metadataInputs){
  const bytes=fs.readFileSync(path.join(physical,input.file));
  check(bytes.length===input.bytes&&sha(bytes)===input.sha256,'SOURCE_PURE_NATIVE_METADATA_SOURCE_CHANGED');
 }
 const definitions=[];
 for(const input of manifest.rootInputs){
  const bytes=fs.readFileSync(path.join(physical,input.file));
  check(sha(bytes)===input.fileSha256,'SOURCE_PURE_SOURCE_CHANGED');
  const selected=extract(bytes,input.name);
  check(selected.length===input.definitionBytes&&sha(selected)===input.definitionSha256,
   'SOURCE_PURE_DEFINITION_CHANGED');
  definitions.push(selected);
 }
 for(const input of manifest.localInputs){
  const bytes=fs.readFileSync(path.join(here,input.file));
  check(bytes.length===input.bytes&&sha(bytes)===input.sha256,'SOURCE_PURE_LOCAL_SOURCE_CHANGED');
 }
 check(definitions[0].equals(fs.readFileSync(path.join(here,'source-child.sqlinc')))
  &&definitions[1].equals(fs.readFileSync(path.join(here,'key-source.sqlinc'))),'SOURCE_PURE_COMPLETE_SOURCE_EQUALITY_REQUIRED');
 const text=definitions.map(bytes=>bytes.toString('utf8')).join('\n');
 const bodies=definitions.map(bytes=>bytes.toString('utf8').split('as $function$')[1].split('$function$;')[0]).join('\n');
 // Exactly two pure Health declarations. The existing key helper is never emitted.
 check(!/\b(?:insert|update|delete|merge|execute|nextval|setval|pg_advisory|current_setting|set_config|pg_notify|dblink)\b/i.test(bodies)
  &&!/\b(?:from|join)\s+(?:private|public)\./i.test(bodies),'SOURCE_PURE_EFFECTFUL_BODY_REFUSED');
 const dependencies=[...new Set([...text.matchAll(/\bprivate\.([a-z_][a-z0-9_]*)\s*\(/g)]
  .map(match=>'private.'+match[1]))].sort();
 check(JSON.stringify(dependencies)===JSON.stringify([...manifest.privateDependencyClosure].sort()),
  'SOURCE_PURE_DEPENDENCY_CLOSURE_CHANGED');
 const read=file=>fs.readFileSync(path.join(here,file),'utf8');
 const child=definitions[0].toString('utf8').replace('create or replace function ',()=> 'create function ');
 const revoke='revoke all on function private.weekly_source_pay_decision_native_status_child_v1(jsonb,uuid,uuid,uuid,uuid) from public,anon,authenticated,service_role;';
 const sql=read('preflight.sql')+'\n'+child+'\n'+revoke+'\n\n'+read('postflight.sql')+'\n'+read('assertions.sql');
 check(Buffer.byteLength(sql)===manifest.sqlBytes&&sha(Buffer.from(sql))===manifest.sqlSha256
  &&sql===read('PROGRAMME.sql'),'SOURCE_PURE_EXACT_PROGRAMME_CHANGED');
 check((sql.match(/create function /g)||[]).length===1&&!/create or replace function/i.test(sql)
  &&!sql.includes(definitions[1].toString('utf8')),'SOURCE_PURE_EXISTING_DEPENDENCY_REPLACEMENT_REFUSED');
 return Object.freeze({sql,sqlSha256:manifest.sqlSha256,sqlBytes:manifest.sqlBytes,
  definitionCount:1,positiveBoundaryVectors:4,negativeBoundaryRefusals:12,
  businessFixtureRows:0,existingDependencyReplaced:false,publicResultSuccess:'NOT_PROVIDED',
  native:'REQUIRED_NOT_RUN',restoration:'REQUIRED_NOT_RUN'});
}

if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 throw Error('SOURCE_PURE_SOURCE_ONLY_NO_EXECUTION_ADAPTER');
}
