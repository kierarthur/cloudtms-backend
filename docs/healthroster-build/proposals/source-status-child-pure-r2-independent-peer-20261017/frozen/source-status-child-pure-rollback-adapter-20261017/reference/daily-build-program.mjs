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
 check(start>=0&&text.indexOf(prefix,start+1)===-1,'DAILY_PURE_DEFINITION_UNIQUE_REQUIRED');
 const body=text.indexOf('as $function$',start);
 const end=text.indexOf('$function$;',body+'as $function$'.length);
 check(body>start&&end>body,'DAILY_PURE_DEFINITION_BOUNDARY_REQUIRED');
 return Buffer.from(text.slice(start,end+'$function$;'.length));
}

export function buildPureProgram({root}){
 check(typeof root==='string'&&path.isAbsolute(root),'DAILY_PURE_ABSOLUTE_ROOT_REQUIRED');
 const physical=fs.realpathSync.native(root).replaceAll('\\','/');
 check(physical==='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',
  'DAILY_PURE_REGISTERED_ROOT_REQUIRED');
 const definitions=[];
 for(const input of manifest.rootInputs){
  const bytes=fs.readFileSync(path.join(physical,input.file));
  check(sha(bytes)===input.fileSha256,'DAILY_PURE_SOURCE_CHANGED');
  const selected=extract(bytes,input.name);
  check(selected.length===input.definitionBytes&&sha(selected)===input.definitionSha256,
   'DAILY_PURE_DEFINITION_CHANGED');
  definitions.push(selected);
 }
 const helper=fs.readFileSync(path.join(here,manifest.helper.file));
 check(helper.length===manifest.helper.bytes&&sha(helper)===manifest.helper.sha256,
  'DAILY_PURE_HELPER_CHANGED');
 check(extract(helper,manifest.helper.name).equals(helper),'DAILY_PURE_HELPER_BOUNDARY_CHANGED');
 definitions.push(helper);
 const assertions=fs.readFileSync(path.join(here,manifest.program.file));
 check(assertions.length===manifest.program.bytes&&sha(assertions)===manifest.program.sha256,
  'DAILY_PURE_ASSERTIONS_CHANGED');
 const declarations=Buffer.concat(definitions.flatMap(bytes=>[bytes,Buffer.from('\n\n')]));
 const text=declarations.toString('utf8');
 // This finite closure never consults an application table, mutable actor,
 // financial producer, payment/provider, outbox, network or dynamic executor.
 check(!/\b(?:insert|update|delete|merge|execute|perform\s+pg_|nextval|setval)\b/i.test(text),
  'DAILY_PURE_EFFECTFUL_BODY_REFUSED');
 const dependencies=[...new Set([...text.matchAll(/\bprivate\.([a-z_][a-z0-9_]*)\s*\(/g)]
  .map(match=>'private.'+match[1]))].sort();
 check(JSON.stringify(dependencies)===JSON.stringify([...manifest.privateDependencyClosure].sort()),
  'DAILY_PURE_DEPENDENCY_CLOSURE_CHANGED');
 const sql=Buffer.concat([declarations,assertions]).toString('utf8');
 return Object.freeze({sql,sqlSha256:sha(Buffer.from(sql)),sqlBytes:Buffer.byteLength(sql),
  definitionCount:definitions.length,positiveUnicodeVectors:manifest.program.positiveUnicodeVectors,
  malformedStoredReasonRefusals:manifest.program.malformedStoredReasonRefusals,
  businessFixtureRows:0,native:'REQUIRED_NOT_RUN',restoration:'REQUIRED_NOT_RUN'});
}

if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 throw Error('DAILY_PURE_SOURCE_ONLY_NO_EXECUTION_ADAPTER');
}
