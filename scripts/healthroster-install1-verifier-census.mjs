// Source-only census. No database, credentials, subprocess, or network access.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {inventory} from './cloudtms-db-release-lib.mjs';
import {canonical,digest,scanManagedSql} from './cloudtms-managed-release-sql.mjs';
import {routineDefinitions} from './bpay-next-final-definers.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');

export function verifierClosure(file,base=root,stack=[]){
 if(!/^(?:supabase\/(?:verification|repeatable|migrations|baseline)\/|tests\/)[A-Za-z0-9_./-]+\.(?:sql(?:inc)?|inc)$/i.test(file)
  ||file.split('/').includes('..')||stack.includes(file))throw Error('REHEARSAL_INCLUDE_SCOPE_REFUSED '+file);
 const source=canonical(fs.readFileSync(path.join(base,file),'utf8'));
 const parsed=scanManagedSql(source),files=[{file,sha256:digest(source),bytes:Buffer.byteLength(source)}],replacements=[];
 for(const meta of parsed.meta.filter(row=>/^\\ir\s/i.test(row.text))){
  const match=meta.text.match(/^\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/i);
  if(!match)throw Error('REHEARSAL_INCLUDE_COMMAND_REFUSED');
  const child=path.posix.normalize(path.posix.join(path.posix.dirname(file),match[1]??match[2]??match[3]));
  const closure=verifierClosure(child,base,[...stack,file]);files.push(...closure.files);replacements.push({...meta,value:closure.expanded});
 }
 let expanded=source;for(const row of replacements.reverse())expanded=expanded.slice(0,row.start)+row.value+expanded.slice(row.end);
 return {file,source,expanded,files,sourceSqlHash:digest(expanded),closureHash:digest(JSON.stringify(files))};
}

// Keep dollar-quoted SQL/PLpgSQL code; mask comments and ordinary literals.
// Literal routine identities are retained separately as catalogue references.
export function executableText(sql,literalSink=[]){
 let out='',i=0;
 while(i<sql.length){
  if(sql.startsWith('--',i)){const e=sql.indexOf('\n',i);const end=e<0?sql.length:e;out+=' '.repeat(end-i);i=end;continue;}
  if(sql.startsWith('/*',i)){let d=1,j=i+2;while(j<sql.length&&d){if(sql.startsWith('/*',j)){d++;j+=2;}else if(sql.startsWith('*/',j)){d--;j+=2;}else j++;}if(d)throw Error('CENSUS_COMMENT_UNCLOSED');out+=' '.repeat(j-i);i=j;continue;}
  if(sql[i]==="'"){let j=i+1;while(j<sql.length){if(sql[j]==="'"){if(sql[j+1]==="'"){j+=2;continue;}j++;break;}if(sql[j]==='\\'&&/[eE]/.test(sql[i-1]??''))j+=2;else j++;}literalSink.push(sql.slice(i+1,j-1).replaceAll("''","'"));out+=' '.repeat(j-i);i=j;continue;}
  const tag=sql.slice(i).match(/^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/)?.[0];
  if(tag){out+=' '.repeat(tag.length);i+=tag.length;continue;}
  out+=sql[i++];
 }
 return out;
}
const unique=xs=>[...new Set(xs)].sort();
export function effectCensus(sql){
 const literals=[],code=executableText(sql,literals);
 const writes=unique([...code.matchAll(/\b(?:insert\s+into|update|delete\s+from|truncate(?:\s+table)?)\s+((?:public|private|pg_temp)\.[a-z_][a-z0-9_]*)/gi)].map(x=>x[1].toLowerCase()));
 const declarations=unique([...code.matchAll(/\b(?:create\s+(?:(?:temporary|temp|unlogged)\s+)?table(?:\s+if\s+not\s+exists)?|references)\s+((?:public|private|pg_temp)\.[a-z_][a-z0-9_]*)\s*\(/gi)].map(x=>x[1].toLowerCase()));
 return {calls:unique([...code.matchAll(/\b((?:public|private|pg_temp|net|http|cron|extensions)\.[a-z_][a-z0-9_]*)\s*\(/gi)].map(x=>x[1].toLowerCase())).filter(name=>!writes.includes(name)&&!declarations.includes(name)),
  literalRoutineReferences:unique(literals.flatMap(value=>{
   const relations=new Set([...value.matchAll(/\b(?:insert\s+into|update|delete\s+from|truncate(?:\s+table)?|create\s+(?:(?:temporary|temp|unlogged)\s+)?table(?:\s+if\s+not\s+exists)?|references)\s+((?:public|private)\.[a-z_][a-z0-9_]*)\b/gi)].map(x=>x[1].toLowerCase()));
   return [...value.matchAll(/\b((?:public|private)\.[a-z_][a-z0-9_]*)\s*\(/gi)].map(x=>x[1].toLowerCase()).filter(name=>!relations.has(name));
  })),
  writes,
  dynamicExecute:/\bexecute\b/i.test(code),
  networkCalls:unique([...code.matchAll(/\b((?:net|http|extensions)\.[a-z_][a-z0-9_]*(?:http|request|send)[a-z0-9_]*)\s*\(/gi)].map(x=>x[1].toLowerCase())),
  sequenceCalls:/\b(?:nextval|setval)\s*\(/i.test(code)};
}
export const potentialCalleeNames=effects=>unique([...effects.calls,...effects.dynamicExecute?effects.literalRoutineReferences:[]]);

export function buildVerifierCensus(base=root){
 if(base!==root)throw Error('CENSUS_LIBRARY_ROOT_MISMATCH');
 const current=inventory(),definitions=new Map(),triggers=new Map(),dictionary=new Map();
 const release=JSON.parse(fs.readFileSync(path.join(base,'supabase/release/current-release.json'),'utf8'));
 for(const item of [...release.baselineFiles.map(path=>({path})),...current.migrations,...current.repeatables]){
  const closure=verifierClosure(item.path,base);
  for(const row of scanManagedSql(closure.expanded).statements){
   const statement=closure.expanded.slice(row.start,row.end);
   for(const signature of routineDefinitions(statement,item.path)){
    const name=signature.slice(0,signature.indexOf('('));
    const record={signature,name,root:item.path,sourceFileHashes:closure.files,statementSha256:digest(statement),...effectCensus(statement)};
    if(!definitions.has(name))definitions.set(name,new Map());definitions.get(name).set(signature,record);
   }
   // Source-owned compatibility renames create callable aliases without a
   // CREATE FUNCTION declaration. Retain predecessor plus exact rename hashes;
   // the fresh catalogue remains responsible for the actually installed body.
   for(const rename of statement.matchAll(/\balter\s+function\s+((?:public|private)\.[a-z_][a-z0-9_]*)\(([^)]*)\)\s+(rename\s+to|set\s+schema)\s+([a-z_][a-z0-9_]*)/gi)){
    const previous=rename[1].toLowerCase(),args=rename[2].toLowerCase().replace(/\s+/g,''),signature=previous+'('+args+')';
    const old=definitions.get(previous)?.get(signature);if(!old)continue;
    const name=/^set/i.test(rename[3])?rename[4].toLowerCase()+previous.slice(previous.indexOf('.')):
     previous.slice(0,previous.indexOf('.')+1)+rename[4].toLowerCase(),renamedSignature=name+'('+args+')';
    if(!definitions.has(name))definitions.set(name,new Map());
    if(!definitions.get(name).has(renamedSignature))definitions.get(name).set(renamedSignature,{...old,name,signature:renamedSignature,
     root:item.path,sourceFileHashes:[...old.sourceFileHashes,...closure.files],
     statementSha256:digest(old.statementSha256+'\0'+statement),renamedFrom:signature,renameStatementSha256:digest(statement),predecessorStatementSha256:old.statementSha256});
   }
   const m=statement.match(/\bcreate\s+(?:or\s+replace\s+)?(?:constraint\s+)?trigger\s+([a-z_][a-z0-9_]*)[\s\S]*?\bon\s+((?:public|private)\.[a-z_][a-z0-9_]*)[\s\S]*?execute\s+(?:function|procedure)\s+((?:public|private)\.[a-z_][a-z0-9_]*)/i);
   if(m){if(!triggers.has(m[2].toLowerCase()))triggers.set(m[2].toLowerCase(),new Map());triggers.get(m[2].toLowerCase()).set(m[1],{name:m[3].toLowerCase(),root:item.path,statementSha256:digest(statement)});}
  }
 }
 const verifiers=release.verificationFiles.map((file,index)=>{
  const closure=verifierClosure(file,base),effects=effectCensus(closure.expanded),
   queue=potentialCalleeNames(effects),visited=new Set(),callees=[],unknown=[];
  const fixtureDefinitions=new Map();
  for(const statement of scanManagedSql(closure.expanded).statements){
   const sql=closure.expanded.slice(statement.start,statement.end);
   for(const signature of routineDefinitions(sql,file)){const name=signature.slice(0,signature.indexOf('('));fixtureDefinitions.set(name,{signature,name,root:file,statementSha256:digest(sql),...effectCensus(sql)});}
   // Assertion-control trigger functions can be defined inside a DO block.
   // Capture their complete enclosing statement instead of silently losing them.
   for(const match of sql.matchAll(/\bcreate\s+(?:or\s+replace\s+)?function\s+(pg_temp\.[a-z_][a-z0-9_]*)\(\)/gi)){
    const name=match[1].toLowerCase();if(!fixtureDefinitions.has(name))fixtureDefinitions.set(name,{signature:name+'()',name,root:file,
     statementSha256:digest(sql),enclosingDoStatement:true,...effectCensus(sql)});
   }
  }
  const addTriggers=writes=>{for(const table of writes)for(const trigger of triggers.get(table)?.values()??[])queue.push(trigger.name);};
  addTriggers(effects.writes);
  while(queue.length){const name=queue.shift();if(visited.has(name))continue;visited.add(name);
   const candidates=fixtureDefinitions.has(name)?[fixtureDefinitions.get(name)]:[...definitions.get(name)?.values()??[]];
   if(!candidates.length){unknown.push(name);continue;}
   for(const record of candidates){const key=record.signature+'@'+record.statementSha256;dictionary.set(key,record);callees.push(key);
    // EXECUTE can call a source-owned routine through an ordinary SQL literal,
    // or an assertion helper can execute a caller's literal. Keep the whole
    // potential literal edge set whenever dynamic execution is present; a
    // reviewer then distinguishes executed SQL from catalogue/negative probes.
    // A literal is never silently treated as inert merely because it was masked.
    queue.push(...potentialCalleeNames(record).filter(x=>x!==name));addTriggers(record.writes);}
  }
  const parsed=scanManagedSql(closure.expanded);
  return {index,file,sourceSqlHash:closure.sourceSqlHash,closureHash:closure.closureHash,files:closure.files,bytes:Buffer.byteLength(closure.expanded),
   statements:parsed.statements.length,meta:parsed.meta.map(x=>x.text),
   envelopes:parsed.statements.filter(x=>['BEGIN','COMMIT','ROLLBACK','END','START','ABORT','PREPARE'].includes(x.tokens[0])&&!(x.tokens[0]==='ROLLBACK'&&x.tokens[1]==='TO')).map(x=>x.tokens.join(' ')),
   ...effects,callees,unknownCalls:unique(unknown),
   effectGraphHash:digest(JSON.stringify(callees)),
   applyingReferences:/\bAPPLYING\b/.test(closure.expanded),
   prohibitedCandidates:unique(callees.flatMap(key=>{const x=dictionary.get(key);return [x.name,...x.networkCalls];}).filter(x=>/(?:claim_ready_batch|projection_claim|dirty_apply_jobs_chunk|settled_certificate_(?:due_claim|build_start|append|seal)|pending_entitlement_release_(?:claim|apply)|pay_batch_finalize_reservations|provider_submit|http_|email_send|message_dispatch_(?:target_)?claim|expense_summary_claim)/i.test(x)))};
 });
 return {status:'SOURCE_CENSUS_ONLY_NOT_EXECUTION_AUTHORITY',sourceCounts:{migrations:current.migrations.length,repeatables:current.repeatables.length,verifiers:verifiers.length},
  newVerifierParity:release.newVerificationFiles.includes('supabase/verification/13102026_1126_weekly_source_protected_pay_authority_verification.sql'),
  definitionNames:definitions.size,definitionVariants:[...definitions.values()].reduce((n,x)=>n+x.size,0),calleeDictionary:Object.fromEntries(dictionary),verifiers};
}

if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const census=buildVerifierCensus(),output=process.argv[2];if(!output)throw Error('SOURCE_CENSUS_OUTPUT_REQUIRED');
 fs.writeFileSync(output,JSON.stringify(census,null,2)+'\n');
 console.log(JSON.stringify({status:census.status,counts:census.sourceCounts,definitionNames:census.definitionNames,
  rows:census.verifiers.map(x=>({index:x.index,file:x.file,bytes:x.bytes,statements:x.statements,callees:x.callees.length,unknownCount:x.unknownCalls.length,prohibited:x.prohibitedCandidates,applying:x.applyingReferences,envelopes:x.envelopes}))}));
}
