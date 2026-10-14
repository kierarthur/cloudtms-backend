import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];assert.ok(root);
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),inputs=new Map(),occurrences=[],definitions=new Map(),rawDefinitions=new Map(),unresolved=[];
function read(file){const b=fs.readFileSync(path.join(root,file));inputs.set(file,{path:file,bytes:b.length,sha256:sha(b)});return b.toString('utf8');}
const release=JSON.parse(read('supabase/release/current-release.json'));
const catalogue=JSON.parse(read('supabase/release/current-contract.json'));
read('scripts/cloudtms-db-release-lib.mjs');
function validDate(y,m,d){const v=new Date(Date.UTC(y,m-1,d));return v.getUTCFullYear()===y&&v.getUTCMonth()===m-1&&v.getUTCDate()===d;}
// Same reviewed source date/identity ordering as the native release library.
function key(file){const b=path.basename(file);let m=b.match(/^(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(.*)$/);
 if(m&&validDate(+m[1],+m[2],+m[3]))return `${m[1]}${m[2]}${m[3]}${m[4]}`;
 m=b.match(/^(0[1-9]|[12]\d|3[01])(0[1-9]|1[0-2])(20\d{2})(.*)$/);if(m&&validDate(+m[3],+m[2],+m[1]))return `${m[3]}${m[2]}${m[1]}${m[4]}`;return 'ZZZZZZZZ_'+b;}
function roots(dir){const g=spawnSync('git',['ls-files','-z','--',dir],{cwd:root,encoding:'utf8',windowsHide:true});assert.equal(g.status,0);
 const tracked=g.stdout.split('\0').filter(p=>p.toLowerCase().endsWith('.sql'));
 const disk=fs.readdirSync(path.join(root,dir),{withFileTypes:true}).filter(e=>e.isFile()&&e.name.endsWith('.sql')).map(e=>dir+'/'+e.name);
 return [...new Set([...tracked,...disk])].sort((a,b)=>key(a).localeCompare(key(b))||a.localeCompare(b));}
const migrationRoots=roots('supabase/migrations'),repeatableRoots=roots('supabase/repeatable');let sequence=0;
function maskComments(s){return s.replace(/\/\*[\s\S]*?\*\//g,' ').replace(/--[^\r\n]*/g,' ');}
function inputIdentity(declaration){
 let depth=0,brackets=0,quote='',start=1,args=[],end=-1;
 for(let i=0;i<declaration.length;i++){const ch=declaration[i];
  if(quote){if(ch===quote&&declaration[i+1]===quote)i++;else if(ch===quote)quote='';continue;}
  if(ch==="'"||ch==='"'){quote=ch;continue;}if(ch==='[')brackets++;else if(ch===']')brackets--;
  if(ch==='(')depth++;else if(ch===')'){depth--;if(depth===0){args.push(declaration.slice(start,i));end=i;break;}}
  else if(ch===','&&depth===1&&brackets===0){args.push(declaration.slice(start,i));start=i+1;}
 }
 assert.ok(end>=0,'Unclosed function declaration');
 return args.filter(a=>a.trim()&&!/^\s*out\b/i.test(a)).map(a=>{
  const v=a.replace(/^\s*(?:inout|in|variadic)\s+/i,'').replace(/\s+default\b[\s\S]*$/i,'').trim();
  const words=v.split(/\s+/);const type=words.length>1?words.slice(1).join(' ').toLowerCase():v.toLowerCase();
  return type.replace(/\btimestamptz\b/g,'timestamp with time zone').replace(/\bint4\b/g,'integer').replace(/\bbool\b/g,'boolean');
 }).join(',');
}
function scan(file,text){const defs=[...text.matchAll(/create\s+(?:or\s+replace\s+)?function\s+([\w.]+)\s*(\([\s\S]*?)\bas\s+(\$\w*\$)([\s\S]*?)\3\s*;/gi)];
 for(const d of defs){const name=d[1].toLowerCase();const inputTypes=inputIdentity(d[2]);const header=d[2].replace(/\s+/g,' ').trim();
  const id=name+'('+inputTypes+')';const body=maskComments(d[4]);const hits=[...body.matchAll(/\b(insert\s+into|update|delete\s+from)\s+(?:public\.)?(timesheet_validations|invoice_lines|timesheets|candidates)\b/gi)];
  const item={file,name,inputTypes,header,sequence:sequence++,definitionSha256:sha(d[0]),line:text.slice(0,d.index).split('\n').length,
   firstCreatorTables:[...new Set(hits.filter(h=>/^insert/i.test(h[1])).map(h=>h[2].toLowerCase()))],
   writerTables:[...new Set(hits.map(h=>h[2].toLowerCase()))],
   mutations:hits.map(h=>({table:h[2].toLowerCase(),operation:h[1].toUpperCase(),bodyLine:body.slice(0,h.index).split('\n').length})),
   nativeGateCalls:[...new Set([...body.matchAll(/\b((?:public|private)\.(?:weekly_source_(?:candidate_serial_gate|lock_family_rows|lock_and_resolve_families)_v1|_pay_workbench_candidate_serial_try_gate|_pay_timesheet_rotation_scope))\s*\(/g)].map(h=>h[1]))],
   parentLocks:[...body.matchAll(/\b(?:from|join)\s+public\.timesheets\b[\s\S]{0,450}?for\s+(?:update|(?:key\s+)?share)(?:\s+nowait)?/gi)].map(h=>h[0]),
   callees:[...new Set([...body.matchAll(/\b((?:public|private)\.\w+)\s*\(/g)].map(h=>h[1]))].sort(),
   dynamicExecute:/\bexecute\b/i.test(body),status:'STATIC_SOURCE_ONLY_NOT_ADMISSION_OR_INSTALLED_PROOF'};
  definitions.set(id,item);rawDefinitions.set(id,d[0]);if(hits.length)occurrences.push(item);
 }
 // Direct/top-level dynamic SQL must be reviewed separately; string matching
 // never asserts it executes or that absence of a literal is no writer proof.
 if(/\bexecute\b(?!\s+(?:function|procedure)\b)[\s\S]{0,250}?(?:insert\s+into|update)\s+(?:public\.)?(?:timesheet_validations|invoice_lines|timesheets|candidates)\b/i.test(maskComments(text)))
  unresolved.push({file,reason:'Dynamic text intersects target tables: manual invocation/identity review required.'});
}
function expand(file,allowed,stack=[]){assert.ok(!stack.includes(file),'include cycle '+file);const absolute=path.resolve(root,file),limit=path.resolve(root,allowed);
 assert.ok(absolute===limit||absolute.startsWith(limit+path.sep),'include escape '+file);const text=read(file),lines=text.split(/\r?\n/);let part='';
 for(const line of lines){const m=line.match(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/);
  if(m){scan(file,part);part='';const child=path.relative(root,path.resolve(path.dirname(absolute),m[1]??m[2]??m[3])).replaceAll('\\','/');expand(child,allowed,[...stack,file]);}
  else part+=line+'\n';}scan(file,part);}
for(const f of release.baselineFiles||[])scan(f,read(f));
// Migrations can expressly include baseline pages. This audit follows only
// actual saved Supabase source; it does not alter managed release closure rules.
for(const f of migrationRoots)expand(f,'supabase');
for(const f of repeatableRoots)expand(f,'supabase/repeatable');
const controlling=[...definitions.values()].filter(x=>x.writerTables.length).sort((a,b)=>a.sequence-b.sequence);
const tables=['timesheet_validations','invoice_lines','timesheets','candidates'];
const triggers=catalogue.triggers.filter(t=>t.schema==='public'&&tables.includes(t.table));
const beforeNames=triggers.filter(t=>/\bBEFORE\b/i.test(t.definition)).map(t=>{
 const m=t.definition.match(/EXECUTE FUNCTION ([\w.]+)\(/i);assert.ok(m,t.name);return m[1].includes('.')?m[1]:'public.'+m[1];
});
const extraNames=['private.weekly_source_candidate_serial_gate_v1','private.weekly_source_lock_family_rows_v1',
 'public._pay_workbench_candidate_serial_try_gate','public._pay_workbench_candidate_serial_key','public._pay_timesheet_rotation_scope',
 'private.bpay_source_family_try_lock_v1','public.tsfin_load_context_batch','private.timesheet_correction_signed_witness_v1',
 'private.timesheet_correction_origin_v1','public.timesheet_correction_send_v1'];
const nativeOwners=[...new Set([...beforeNames,...extraNames])].map(name=>{
 const matches=[...definitions.entries()].filter(([,d])=>d.name===name);assert.equal(matches.length,1,'Exact native owner '+name);
 const [id,d]=matches[0];return {...d,catalogue:catalogue.routines.filter(r=>r.schema+'.'+r.identity.slice(0,r.identity.indexOf('('))===name),raw:rawDefinitions.get(id)};
});
fs.writeFileSync(path.join(here,'NATIVE_CREATOR_INPUT_BODIES.sqlinc'),nativeOwners.map(d=>'-- INPUT ONLY '+d.file+' / '+d.name+'\n'+d.raw).join('\n\n')+'\n');
fs.writeFileSync(path.join(here,'NATIVE_FIRST_CREATOR_BODIES.sqlinc'),controlling.filter(d=>d.firstCreatorTables.length)
 .map(d=>'-- INPUT ONLY '+d.file+' / '+d.name+'\n'+rawDefinitions.get(d.name+'('+d.inputTypes+')')).join('\n\n')+'\n');
const index=read('broker/src/index.js'),http=[];
for(const m of index.matchAll(/\/rest\/v1\/(timesheet_validations|invoice_lines|timesheets|candidates)(?![_a-z])/g)){
 const nearby=index.slice(Math.max(0,m.index-300),m.index+900),method=nearby.match(/method:\s*['"](POST|PATCH|PUT|DELETE)['"]/i)?.[1]??'READ_OR_DELEGATED_UNKNOWN';
 if(method!=='READ_OR_DELEGATED_UNKNOWN')http.push({table:m[1],line:index.slice(0,m.index).split('\n').length,method,callSiteSha256:sha(nearby),status:'EXACT_SOURCE_CANDIDATE_REQUIRES_OWNER_REVIEW'});
}
const result={contract:'HEALTHROSTER_A0_FOUR_TABLE_FIRST_CREATOR_SOURCE_CENSUS_V1',status:'SOURCE_BOUNDARIES_WITH_UNRESOLVED_ADMISSION_GATES',
 nativeOrderingInput:'scripts/cloudtms-db-release-lib.mjs',migrationRoots:migrationRoots.length,repeatableRoots:repeatableRoots.length,
 inputs:[...inputs.values()].sort((a,b)=>a.path.localeCompare(b.path)),controllingWriterDefinitions:controlling,
 firstCreators:controlling.filter(d=>d.firstCreatorTables.length),allOverwrittenWriterOccurrences:occurrences,httpCandidates:http,unresolved,
 nativeOwners:nativeOwners.map(({raw,...d})=>d),triggers,
 limitations:['Signature identity uses exact saved declaration; overload/drop/dynamic rewrites still require current generated catalogue and native installer review.','Every source boundary is recorded; a literal match does not mean a branch is admitted or invoked.','No visible-request predicate/row census claims absent-row isolation.','No DB/API/runtime/DDL/email/payment/provider/drain call was executed.']};
fs.writeFileSync(path.join(here,'FIRST_CREATOR_CENSUS.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({inputs:inputs.size,migrations:migrationRoots.length,repeatables:repeatableRoots.length,controllingWriters:controlling.length,
 firstCreators:result.firstCreators.length,httpCandidates:http.length,unresolved:unresolved.length}));
