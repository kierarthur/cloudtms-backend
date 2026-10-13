// Offline source inventory only. No connection, environment or network API.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
export const hash = x => crypto.createHash('sha256').update(x).digest('hex');
export const normal = x => x.replaceAll('\\', '/');
export function argumentTypes(args) {
  const parts=[];let start=0,depth=0,quote=false;
  for(let i=0;i<args.length;i++){
    if(args[i]==="'"){if(quote&&args[i+1]==="'"){i++;continue;}quote=!quote;}
    if(!quote){if(args[i]==='(')depth++;if(args[i]===')')depth--;
      if(args[i]===','&&depth===0){parts.push(args.slice(start,i));start=i+1;}}
  }
  if(args.slice(start).trim())parts.push(args.slice(start));
  return parts.filter(p=>!/^\s*out\s/i.test(p)).map(p=>{
    const words=p.trim().replace(/\s+(?:default|=)[\s\S]*$/i,'').replace(/^(?:in|inout|variadic)\s+/i,'').split(/\s+/);
    const first=words[0].toLowerCase();
    // Function arguments use named p_* values or bare type declarations.
    const primitive=/^(?:uuid|jsonb?|text|boolean|bool|int[248]?|integer|bigint|smallint|numeric|decimal|real|double|date|timestamp|time|interval|bytea|record|any\w+|character|varchar|citext|\w+\.\w+)(?:\[\])?(?:\([^)]*\))?$/i;
    return (primitive.test(first)?words:words.slice(1)).join(' ').toLowerCase();
  });
}
export function lexical(input) {
  let code = '', i = 0; const literals = [];
  while (i < input.length) {
    if (input.startsWith('--', i)) { const j=input.indexOf('\n',i); const end=j<0?input.length:j; code+=' '.repeat(end-i); i=end; continue; }
    if (input.startsWith('/*', i)) { const start=i; let d=1; i+=2;
      while(i<input.length && d) { if(input.startsWith('/*',i)){d++;i+=2;}else if(input.startsWith('*/',i)){d--;i+=2;}else i++; }
      code+=input.slice(start,i).replace(/[^\n]/g,' '); continue; }
    if (input[i]==="'") { const start=i++; let value='';
      while(i<input.length) { if(input[i]==="'" && input[i+1]==="'"){value+="'";i+=2;}
        else if(input[i]==="'"){i++;break;}else value+=input[i++]; }
      literals.push({start,end:i,value}); code+=input.slice(start,i).replace(/[^\n]/g,' '); continue; }
    // Inspect dollar bodies as code; nested dynamic dollar literals are retained
    // separately by the EXECUTE census below. This is conservative, not a parser.
    code+=input[i++];
  }
  return {code,literals};
}
export function calls(input) {
  const {code,literals}=lexical(input);
  const direct=[...new Set([...code.matchAll(/\b((?:public|private|pg_temp|net|http|extensions)\.[a-z_][a-z_0-9]*)\s*\(/gi)].map(m=>m[1].toLowerCase()))].sort();
  const dynamic=literals.filter(x=>/^\s*(?:select|perform|call|insert|update|delete|create|alter|drop)\b/i.test(x.value))
    .map(x=>({line:input.slice(0,x.start).split('\n').length,sql:x.value,preceding:input.slice(Math.max(0,x.start-100),x.start).replace(/\s+/g,' ')}));
  const executedDynamic=dynamic.filter(x=>/\bexecute\s+(?:format\s*\(\s*)?$/i.test(x.preceding));
  const dynamicCalls=[...new Set(executedDynamic.flatMap(x=>[...x.sql.matchAll(/\b((?:public|private|pg_temp|net|http|extensions)\.[a-z_][a-z_0-9]*)\s*\(/gi)].map(m=>m[1].toLowerCase())))].sort();
  const writes=[...new Set([...code.matchAll(/\b(?:insert\s+into|update|delete\s+from)\s+((?:public|private|pg_temp)\.[a-z_][a-z_0-9]*)\b/gi)].map(m=>m[1].toLowerCase()))].sort();
  return {direct,dynamicCalls,writes,dynamic:dynamic.map(x=>({...x,sqlSha256:hash(x.sql),sql:x.sql.slice(0,220)})),
    executeLines:input.split('\n').flatMap((line,i)=>/\bexecute\b/i.test(lexical(line).code)?[{line:i+1,text:line.trim()}]:[])};
}
export async function inventory(root) {
  const moduleUrl=pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs'));
  const {readManagedClosure,scanManagedSql,exactAtomicBody}=await import(moduleUrl);
  const {sqlFiles}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-db-release-lib.mjs')));
  const release=JSON.parse(fs.readFileSync(path.join(root,'supabase/release/current-release.json')));
  const roots=[...new Set([...release.baselineFiles,...sqlFiles('supabase/migrations'),...sqlFiles('supabase/repeatable')])];
  const functions=new Map(), triggers=new Map(), sources=new Map(), relations=new Set();
  for(const rootPath of roots) {
    const closure=readManagedClosure(rootPath,root);
    for(const leaf of closure.ordered) {
      sources.set(leaf.path,{path:leaf.path,bytes:Buffer.byteLength(leaf.source),sha256:hash(fs.readFileSync(path.join(root,leaf.path))),canonicalSha256:hash(leaf.source)});
      const parsed=scanManagedSql(leaf.source);
      for(const statement of parsed.statements) {
        const sql=parsed.source.slice(statement.start,statement.end);
        const relation=sql.match(/^create\s+(?:or\s+replace\s+)?(?:table|(?:materialized\s+)?view)\s+(?:if\s+not\s+exists\s+)?((?:public|private)\.[a-z_0-9]+)/i);
        if(relation)relations.add(relation[1].toLowerCase());
        const m=sql.match(/^create\s+(?:or\s+replace\s+)?function\s+((?:public|private)\.[a-z_0-9]+)\s*\(/i);
        if(m) {
          const from=m[0].length;let end=from,depth=1;
          while(end<sql.length && depth){if(sql[end]==='(')depth++;if(sql[end]===')')depth--;end++;}
          const args=sql.slice(from,end-1),types=argumentTypes(args),arity=types.length;
          const key=m[1].toLowerCase()+'('+types.join(',')+')';
          const dollar=sql.match(/\bas\s+(\$(?:[a-z_0-9]*)\$)/i);
          const body=dollar?sql.slice(dollar.index+dollar[0].length,sql.lastIndexOf(dollar[1])):sql;
          functions.set(key,{key,name:m[1].toLowerCase(),arity,types,path:leaf.path,root:rootPath,args,sql,body,
            statementSha256:hash(sql),bodySha256:hash(body),line:parsed.source.slice(0,statement.start).split('\n').length,...calls(body)});
        }
        const t=sql.match(/^create\s+(?:constraint\s+)?trigger\s+([a-z_0-9]+)[\s\S]*?\bon\s+((?:public|private)\.[a-z_0-9]+)[\s\S]*?execute\s+(?:function|procedure)\s+((?:public|private)\.[a-z_0-9]+)\s*\(/i);
        if(t)triggers.set(t[2].toLowerCase()+'/'+t[1],{name:t[1],relation:t[2].toLowerCase(),callee:t[3].toLowerCase(),path:leaf.path,root:rootPath,statementSha256:hash(sql)});
        const d=sql.match(/^drop\s+trigger\s+(?:if\s+exists\s+)?([a-z_0-9]+)\s+on\s+((?:public|private)\.[a-z_0-9]+)/i);
        if(d)triggers.delete(d[2].toLowerCase()+'/'+d[1]);
      }
    }
  }
  return {root,release,roots,functions,triggers,sources,relations,readManagedClosure,scanManagedSql,exactAtomicBody};
}
export function closureCalls(inv,seeds,extraWrites=[]) {
  const byName=new Map(); for(const f of inv.functions.values()){const a=byName.get(f.name)??[];a.push(f);byName.set(f.name,a);}
  const unqualified=new Map();for(const name of byName.keys()){const short=name.split('.')[1];const a=unqualified.get(short)??[];a.push(name);unqualified.set(short,a);}
  const todo=[...seeds],seen=new Set(),found=new Map(),edges=[],unresolved=new Set(),relations=new Set(extraWrites);
  const enqueueTriggers=relation=>{for(const t of inv.triggers.values())if(t.relation===relation){todo.push(t.callee);edges.push({from:relation,to:t.callee,kind:'DML_TRIGGER_CONSERVATIVE',trigger:t.name});}};
  for(const r of extraWrites)enqueueTriggers(r);
  while(todo.length){const name=todo.pop();if(seen.has(name)||name.startsWith('pg_temp.'))continue;seen.add(name);
    const targets=byName.get(name);if(!targets){if(!inv.relations.has(name))unresolved.add(name);continue;}
    for(const f of targets){found.set(f.key,f);
      for(const n of [...f.direct,...f.dynamicCalls]){todo.push(n);edges.push({from:f.key,to:n,kind:f.dynamicCalls.includes(n)?'EXECUTED_LITERAL':'DIRECT_CONSERVATIVE'});}
      const bodyCode=lexical(f.body).code;
      for(const m of bodyCode.matchAll(/(?<![.a-z_0-9])([a-z_][a-z_0-9]*)\s*\(/gi)) {
        for(const n of unqualified.get(m[1].toLowerCase())??[]){todo.push(n);edges.push({from:f.key,to:n,kind:'UNQUALIFIED_NATIVE_CONSERVATIVE'});}
      }
      for(const r of f.writes){relations.add(r);enqueueTriggers(r);}
    }
  }
  return {owners:[...found.values()].map(({sql,body,...f})=>f).sort((a,b)=>a.key.localeCompare(b.key)),
    edges:[...new Map(edges.map(e=>[JSON.stringify(e),e])).values()],
    unresolved:[...unresolved].sort(),mutatedRelations:[...relations].sort()};
}
export function assertRoot(root,commit) {
  const r=spawnSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','rev-parse','HEAD'],{cwd:root,encoding:'utf8'});
  if(r.status!==0)throw Error('REVIEW_BASE_HEAD_UNAVAILABLE');
  const current=r.stdout.trim();
  for(const args of [['diff','--name-only',commit,current,'--','supabase','scripts','broker'],
      ['diff','--name-only',commit,'--','supabase','scripts','broker']]) {
      const d=spawnSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,encoding:'utf8'});
      // Root is concurrently instrumenting its SEND_7 CLI. It is neither
      // imported nor selected by this review. Exact native/loader/Worker pins
      // remain mandatory; no other changed executable source is waived.
      const changed=d.stdout.trim().split(/\r?\n/).filter(Boolean);
      if(d.status!==0||changed.some(p=>p!=='scripts/healthroster-rehearsal-turn-cli.mjs'))throw Error('REVIEW_CONTROLLING_SOURCE_CHANGED');
  }
  return current;
}
export const here=path.dirname(fileURLToPath(import.meta.url));
