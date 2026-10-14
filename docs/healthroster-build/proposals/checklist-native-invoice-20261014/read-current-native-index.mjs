// Source evidence resolver only. No native SQL or database route exists here.
import {execFileSync} from 'node:child_process';import crypto from 'node:crypto';import assert from 'node:assert/strict';
import {scanManagedSql} from '../../../../scripts/cloudtms-managed-release-sql.mjs';
import {functionEvents,applyFunctionEvents,emptyFunctionIndex,sqlTokens} from '../checklist-native-20261014/financial-native-input-20261014/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
import {sqlDateKey} from '../checklist-native-20261014/financial-native-input-20261014/PINNED_SQL_DATE_ORDER.mjs';
export const sourceCommit='fa66183f01913e86651e82816f5e88f97e07123c';
export const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
export function readCurrentNativeIndex(repo){
  const git=(args,input)=>execFileSync('git',['-c','maintenance.auto=false',...args],{cwd:repo,input,maxBuffer:50e6});
  const files=git(['ls-tree','-r','--name-only',sourceCommit,'--','supabase']).toString().trim().split('\n').filter(f=>/\.(sql|sqlinc|inc)$/i.test(f)||f==='supabase/release/current-release.json'),cache=new Map();
  for(let i=0;i<files.length;i+=64){const paths=files.slice(i,i+64),raw=git(['cat-file','--batch'],paths.map(p=>sourceCommit+':'+p+'\n').join(''));let at=0;
    for(const file of paths){const end=raw.indexOf(10,at),header=raw.subarray(at,end).toString().split(' ');assert.equal(header[1],'blob');at=end+1;const bytes=Number(header[2]);cache.set(file,raw.subarray(at,at+bytes));at+=bytes+1;}assert.equal(at,raw.length);}
  const release=JSON.parse(cache.get('supabase/release/current-release.json')),sort=a=>a.sort((x,y)=>sqlDateKey(x).localeCompare(sqlDateKey(y))||x.localeCompare(y));
  const roots=[...release.baselineFiles,...sort(files.filter(f=>/^supabase\/migrations\/.*\.sql$/i.test(f))),...sort(files.filter(f=>/^supabase\/repeatable\/.*\.sql$/i.test(f)))];
  const index=emptyFunctionIndex(),inputs=new Map(),occurrences=[];let order=0;
  function visit(file,stack=[]){assert.ok(!stack.includes(file));const raw=cache.get(file);assert.ok(raw,file);const source=raw.toString().replaceAll('\r\n','\n'),parsed=scanManagedSql(source);
    inputs.set(file,{file,bytes:raw.length,sha256:sha(raw)});occurrences.push(file);
    for(const unit of [...parsed.statements.map(s=>({...s,kind:'statement'})),...parsed.meta.map(s=>({...s,kind:'meta'}))].sort((a,b)=>a.start-b.start)){
      const statement=source.slice(unit.start,unit.end);if(unit.kind==='meta'){const match=statement.match(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))/);
        if(match){const parent=file.slice(0,file.lastIndexOf('/'));const relative=match[1]??match[2]??match[3];const parts=(parent+'/'+relative).split('/'),resolved=[];
          for(const part of parts){if(part==='..')resolved.pop();else if(part!=='.')resolved.push(part);}visit(resolved.join('/'),[...stack,file]);}continue;}
      const events=functionEvents(statement);index.unresolved.push(...events.unresolved.map(e=>({...e,file,order:order++})));
      applyFunctionEvents(index,events.events,e=>({...e,file,include_stack:stack,order:order++,source_file_sha256:sha(raw),definition_sha256:sha(e.definition??statement)}));}}
  for(const root of roots)visit(root);
  const censusBytes=git(['show','973625f17cd51a49803872a819638e10f13bbbff:docs/healthroster-build/proposals/root-effective-body-amendment-census-case-forward-20261014/BODY_AMENDMENT_CENSUS.json']);
  const expected=new Map(JSON.parse(censusBytes).sourceInputs.map(p=>[p.file,p]));assert.deepEqual([roots.length,inputs.size,occurrences.length],[1484,1745,2160]);
  for(const [file,pin]of inputs){assert.equal(pin.sha256,expected.get(file)?.sha256,file);assert.equal(pin.bytes,expected.get(file)?.bytes,file);}
  const byName=new Map(),byBase=new Map();for(const r of index.live.values()){if(!byName.has(r.name))byName.set(r.name,[]);byName.get(r.name).push(r);
    const base=r.name.split('.').at(-1);if(!byBase.has(base))byBase.set(base,new Set());byBase.get(base).add(r.name);}
  const references=body=>{const t=sqlTokens(body),names=new Set();for(let i=0;i<t.length-3;i++){
    if(['private','public','extensions'].includes(t[i].value)&&t[i+1].value==='.'&&t[i+3].value==='(')names.add(t[i].value+'.'+t[i+2].value);
    if(t[i].kind==='word'&&t[i+1].value==='('&&t[i-1]?.value!=='.')for(const n of byBase.get(t[i].value)||[])names.add(n);}
    return [...names].sort();};
  return {index,cache,byName,references,sourceCommit,censusSha256:sha(censusBytes),roots:roots.length,uniqueInputs:inputs.size,occurrences:occurrences.length};
}
