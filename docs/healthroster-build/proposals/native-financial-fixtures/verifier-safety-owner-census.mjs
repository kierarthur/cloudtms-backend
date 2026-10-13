import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
const root=path.resolve(process.argv[2]);
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const relative=p=>path.relative(root,p).replaceAll('\\','/');
const date=f=>{const m=path.basename(f).match(/^(\d{2})(\d{2})(\d{4})(.*)$/);return m?m[3]+m[2]+m[1]+m[4]:f;};
const defs=new Map();
const visited=[];
function visit(f,stack=[]){
 if(stack.includes(f))throw Error('include cycle');
 const s=fs.readFileSync(f,'utf8');visited.push({file:relative(f),sha256:sha(fs.readFileSync(f))});
 const events=[...s.matchAll(/create\s+(?:or\s+replace\s+)?function\s+((?:public|private|pg_temp)\.[a-z_0-9]+)\s*\([\s\S]*?\bas\s+(\$[a-z_0-9]*\$)([\s\S]*?)\2\s*;/gi)].map(m=>({at:m.index,type:'definition',m}));
 for(const m of s.matchAll(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))/gm))events.push({at:m.index,type:'include',include:m[1]??m[2]??m[3]});
 events.sort((a,b)=>a.at-b.at);
 for(const e of events){if(e.type==='include')visit(path.resolve(path.dirname(f),e.include),[...stack,f]);else defs.set(e.m[1].toLowerCase(),{file:relative(f),line:s.slice(0,e.at).split('\n').length,definition:e.m[0],body:e.m[3]});}
}
// Literal-definition location aid in dated root units and explicit includes.
// This is not a compiled catalogue or an interpreter of dynamic definition patches.
for(const base of ['supabase/baseline','supabase/repeatable']){
 const folder=path.join(root,base);
 for(const f of fs.readdirSync(folder).filter(f=>f.endsWith('.sql')).sort((a,b)=>date(a).localeCompare(date(b))||a.localeCompare(b)))visit(path.join(folder,f));
}
const census=JSON.parse(fs.readFileSync(new URL('VERIFIER_SOURCE_CENSUS_0_55.json',import.meta.url)));
const used=[...new Set(census.flatMap(x=>x.calls).filter(n=>defs.has(n)))].sort();
const out=used.map(name=>{const d=defs.get(name);const executable=d.body.replace(/--[^\n]*/g,' ').replace(/\/\*[\s\S]*?\*\//g,' ').replace(/'(?:''|[^'])*'/g,' ');return {name,file:d.file,line:d.line,source_file_sha256:sha(fs.readFileSync(path.join(root,d.file))),definition_sha256:sha(d.definition),body_bytes:Buffer.byteLength(d.body),callees:[...new Set([...executable.matchAll(/\b(public|private)\.([a-z_][a-z_0-9]*)\s*\(/gi)].map(m=>`${m[1]}.${m[2]}`))].filter(n=>defs.has(n)).sort()};});
fs.writeFileSync(new URL('VERIFIER_OWNER_SOURCE_CENSUS.json',import.meta.url),JSON.stringify({note:'Static location aid; branch review and safety decision remain explicit.',owners:out},null,2)+'\n');
for(const d of out)console.log(`${d.name} ${d.file}:${d.line} ${d.body_bytes}B -> ${d.callees.join(', ')}`);
if(process.argv[3]){const d=defs.get(process.argv[3]);if(!d)throw Error('missing definition');console.log(d.definition);}
