import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
const root=path.resolve(process.argv[2]);
const rel=JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-release.json'),'utf8'));
const hash=b=>crypto.createHash('sha256').update(b).digest('hex');
// Inventory aid only: human full-body review determines safety and branch reachability.
function withoutStrings(s){return s.replace(/--[^\n]*/g,' ').replace(/\/\*[\s\S]*?\*\//g,' ').replace(/'(?:''|[^'])*'/g,' ');}
function calls(s){return [...new Set([...withoutStrings(s).matchAll(/\b(public|private|pg_temp)\.([a-z_][a-z_0-9]*)\s*\(/gi)].map(m=>`${m[1]}.${m[2]}`))].sort();}
function closure(file,seen=new Set()){
 if(seen.has(file))throw Error('Recursive include'); seen.add(file);
 const b=fs.readFileSync(path.join(root,file)),s=b.toString('utf8');
 const included=[...s.matchAll(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))/gm)].map(m=>path.posix.normalize(path.posix.join(path.posix.dirname(file),m[1]??m[2]??m[3])));
 return [{file,bytes:b.length,sha256:hash(b),calls:calls(s)},...included.flatMap(f=>closure(f,new Set(seen)))];
}
const out=rel.verificationFiles.slice(0,56).map((file,index)=>({index,...closure(file)[0],includes:closure(file).slice(1)}));
fs.writeFileSync(new URL('VERIFIER_SOURCE_CENSUS_0_55.json',import.meta.url),JSON.stringify(out,null,2)+'\n');
for(const x of out)console.log(`${x.index} ${x.calls.join(', ')}`);
