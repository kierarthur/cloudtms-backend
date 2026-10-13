import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
const root=process.argv[2];
const release=JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-release.json'),'utf8'));
function codeOnly(s){return s.replace(/\/\*[\s\S]*?\*\//g,' ').replace(/--[^\n]*/g,' ').replace(/'(?:''|[^'])*'/g,"''");}
function inspect(file,seen=new Set()){
  if(seen.has(file))return [];seen.add(file);
  const b=fs.readFileSync(path.join(root,file)),s=b.toString('utf8'),code=codeOnly(s);
  const includes=[...s.matchAll(/^\s*\\i[r]?\s+([^\r\n]+)/gm)].map(m=>path.posix.normalize(path.posix.join(path.posix.dirname(file),m[1].trim().replaceAll('\\','/'))));
  const calls=[...code.matchAll(/\b(public|private|pg_temp|net|http|cron)\.([a-zA-Z_0-9]+)\s*\(/g)].map(m=>({name:m[1]+'.'+m[2],line:s.slice(0,m.index).split('\n').length,context:code.slice(Math.max(0,m.index-48),Math.min(code.length,m.index+110)).replace(/\s+/g,' ')}));
  return [{file,sha256:crypto.createHash('sha256').update(b).digest('hex'),bytes:b.length,includes,calls:[...new Map(calls.map(c=>[c.name,c])).values()],dynamic:[...code.matchAll(/\bexecute\b[\s\S]{0,160}/gi)].map(m=>m[0].replace(/\s+/g,' ')),transactionDirectives:s.split('\n').filter(l=>/^\s*(begin\s*;|rollback\s*;|commit\s*;|\\set ON_ERROR|set local statement_timeout)/i.test(l))},...includes.flatMap(f=>inspect(f,seen))];
}
for(let n=Number(process.argv[3]??56);n<=Number(process.argv[4]??111);n++)console.log(JSON.stringify({index:n,closure:inspect(release.verificationFiles[n])}));
