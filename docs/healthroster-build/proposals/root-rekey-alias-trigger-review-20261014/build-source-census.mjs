// Source-only fixture trigger/callee review. No database or application requests.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {scanManagedSql} from '../../../../scripts/cloudtms-managed-release-sql.mjs';
import {sqlDateKey} from '../../../../scripts/cloudtms-db-release-lib.mjs';
import {effectCensus,potentialCalleeNames} from '../../../../scripts/healthroster-install1-verifier-census.mjs';
import {functionEvents,applyFunctionEvents,emptyFunctionIndex,stripSqlComments} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..');
const sourceCommit=process.argv[2]??'2cb3642ec9e49e9e54187e9a0ae6c1a4ed3a5937';
assert.ok(['2cb3642ec9e49e9e54187e9a0ae6c1a4ed3a5937','f743b81fe537830d81b1de93ec4f9cb0331532a7'].includes(sourceCommit),'SOURCE_COMMIT_SCOPE_REFUSED');
const historicalInstalled=sourceCommit==='f743b81fe537830d81b1de93ec4f9cb0331532a7';
const output=path.join(dir,historicalInstalled?'FIXTURE_TRIGGER_CALLEE_INSTALLED_F743_SOURCE_CENSUS.json':'FIXTURE_TRIGGER_CALLEE_SOURCE_CENSUS_QUALIFIED_BASELINE_FORWARD.json');
assert.ok(!fs.existsSync(output),'EXISTING_REVIEW_REFUSED');
const hash=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=(args,input)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,input,maxBuffer:40e6});
const files=git(['ls-tree','-r','--name-only',sourceCommit,'--','supabase']).toString().trim().split('\n').filter(f=>/\.(sql|sqlinc|inc)$/.test(f)||f==='supabase/release/current-release.json');
const cache=new Map();
for(let i=0;i<files.length;i+=48){const batch=files.slice(i,i+48),raw=git(['cat-file','--batch'],batch.map(f=>sourceCommit+':'+f+'\n').join(''));let cursor=0;
 for(const file of batch){const end=raw.indexOf(10,cursor),header=raw.subarray(cursor,end).toString().split(' ');assert.equal(header[1],'blob',file);const bytes=Number(header[2]);cursor=end+1;cache.set(file,raw.subarray(cursor,cursor+bytes));cursor+=bytes+1;}assert.equal(cursor,raw.length);
}
const release=JSON.parse(cache.get('supabase/release/current-release.json'));
const chronological=rows=>rows.sort((a,b)=>sqlDateKey(a).localeCompare(sqlDateKey(b))||a.localeCompare(b));
const roots=[...release.baselineFiles,...chronological(files.filter(f=>/^supabase\/migrations\/.*\.sql$/.test(f))),...chronological(files.filter(f=>/^supabase\/repeatable\/.*\.sql$/.test(f)))];
const functions=emptyFunctionIndex(),triggers=new Map(),triggerHistory=[],dynamicTriggerDdl=[],schemaStatements=[],sourceInputs=new Map();
let statementOrder=0,occurrences=0;
function visit(file,stack=[]){assert.ok(cache.has(file),file);assert.ok(!stack.includes(file),'INCLUDE_CYCLE');occurrences++;
 const raw=cache.get(file),text=raw.toString().replaceAll('\r\n','\n'),parsed=scanManagedSql(text);
 const savedMatchesGit=fs.existsSync(path.join(root,file))&&hash(fs.readFileSync(path.join(root,file)))===hash(raw);
 if(!historicalInstalled)assert.ok(savedMatchesGit,'SAVED_GIT_SOURCE_DRIFT '+file);
 sourceInputs.set(file,{file,bytes:raw.length,sha256:hash(raw),savedMatchesGit,authority:historicalInstalled?'Exact reviewed installed Git source, explicitly historical relative to working tree':'Current saved/Git source matched'});
 for(const unit of [...parsed.statements.map(r=>({...r,kind:'statement'})),...parsed.meta.map(r=>({...r,kind:'meta'}))].sort((a,b)=>a.start-b.start)){
  const sql=text.slice(unit.start,unit.end);
  if(unit.kind==='meta'){const include=sql.match(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))/);if(include)visit(path.posix.normalize(path.posix.join(path.posix.dirname(file),include[1]??include[2]??include[3])),[...stack,file]);continue;}
  const order=statementOrder++,receipt={file,includeStack:stack,order,statementSha256:hash(sql)};
  const events=functionEvents(sql);functions.unresolved.push(...events.unresolved.map(x=>({...x,...receipt})));
  applyFunctionEvents(functions,events.events,e=>({...e,...receipt}));
  const code=stripSqlComments(sql).trim();
  // pg_dump's baseline deliberately uses unqualified public relation/routine
  // identities. Retain those, and corroborate each target identity against the
  // actual installed catalogue before treating this as fixture authority.
  const qualify=n=>n.includes('.')?n.toLowerCase():'public.'+n.toLowerCase();
  const create=code.match(/^create\s+(?:or\s+replace\s+)?(?:constraint\s+)?trigger\s+("[^"]+"|\w+)\s+([\s\S]+?)\bon\s+((?:(?:public|private)\.)?\w+)\s+([\s\S]*?)\bexecute\s+(?:function|procedure)\s+((?:(?:public|private)\.)?\w+)\s*\(/i);
  const drop=code.match(/^drop\s+trigger\s+(?:if\s+exists\s+)?("[^"]+"|\w+)\s+on\s+((?:(?:public|private)\.)?\w+)/i);
  if(create){const name=create[1].replaceAll('"',''),table=qualify(create[3]),key=table+'.'+name;const row={kind:'CREATE',name,table,events:create[2].toUpperCase(),routine:qualify(create[5]),unqualifiedBaselineIdentity:!create[3].includes('.')||!create[5].includes('.'),definition:sql,...receipt};triggers.set(key,row);triggerHistory.push(row);}
  else if(drop){const name=drop[1].replaceAll('"',''),table=qualify(drop[2]),key=table+'.'+name;triggerHistory.push({kind:'DROP',name,table,removed:triggers.delete(key),...receipt});}
  if(/^do\b/i.test(code)&&/\b(?:create\s+(?:or\s+replace\s+)?(?:constraint\s+)?trigger|drop\s+trigger|alter\s+table[\s\S]*?(?:enable|disable)\s+trigger)\b/i.test(code))dynamicTriggerDdl.push({definition:sql,...receipt});
  if(/^(?:create\s+table|alter\s+table)\b/i.test(code))schemaStatements.push({definition:sql,...receipt});
 }
}
for(const file of roots)visit(file);
const fixtureFile='docs/healthroster-build/proposals/root-rekey-alias-native-first-use-20261014/02_NATIVE_HELPER_FIXTURE_OWNER_CONFLICT_FORWARD.sql';
const fixture=fs.readFileSync(path.join(root,fixtureFile),'utf8');
assert.equal(hash(fixture),'7454f9fc1ac622048b5d15bdd3532aabde10f12fe3e3624bbbf934eb7023c046');
const effects=effectCensus(fixture),tables=new Set(effects.writes);
const fixtureFunctions=functionEvents(fixture);assert.equal(fixtureFunctions.unresolved.length,0);
const localFunctions=new Map(fixtureFunctions.events.filter(x=>x.kind==='CREATE').map(x=>[x.name,x]));
const moneyRoot=path.join(path.dirname(root),'healthroster-v2-money-build-13102026');
const moneyManifest=path.join(moneyRoot,'docs/healthroster-build/proposals/rekey-source-alias-all-owner-successor-20261014/FROZEN_ARTIFACT_MANIFEST.json');
const manifestRaw=fs.readFileSync(moneyManifest);assert.equal(hash(manifestRaw),'400b1b49fe5534b79baccbea9a9e3b7017458b40c84afeb66ebafedced92ff5c');
const manifest=JSON.parse(manifestRaw),helperEntry=manifest.artifacts.find(x=>x.sha256==='3efed0771093465c90faed992811b660b8a6f885bcbe7bfd17adfc36eb16d56d');assert.ok(helperEntry);
const helper=fs.readFileSync(path.join(path.dirname(moneyManifest),helperEntry.file),'utf8');assert.equal(hash(helper),helperEntry.sha256);
const helperFunctions=functionEvents(helper).events.filter(x=>x.kind==='CREATE');assert.equal(helperFunctions.length,1);localFunctions.set(helperFunctions[0].name,helperFunctions[0]);
const queue=[...potentialCalleeNames(effects)],seen=new Set(),callees=[],unknown=[],attached=[];
function attach(writes){for(const table of writes){tables.add(table);for(const row of triggers.values())if(row.table===table){const key=row.table+'.'+row.name;if(!attached.some(x=>x.table+'.'+x.name===key))attached.push(row);queue.push(row.routine);}}}
attach(effects.writes);
while(queue.length){const name=queue.shift();if(seen.has(name))continue;seen.add(name);
 const variants=localFunctions.has(name)?[localFunctions.get(name)]:[...functions.live.values()].filter(x=>x.name===name);
 if(!variants.length){unknown.push(name);continue;}
 for(const row of variants){const effects=effectCensus(row.body),record={id:row.id,file:row.file??fixtureFile,order:row.order??null,definitionSha256:hash(row.definition),bodySha256:hash(row.body),definition:row.definition,...effects};callees.push(record);queue.push(...potentialCalleeNames(effects).filter(x=>x!==name));attach(effects.writes);}
}
const selectedSchema=schemaStatements.filter(x=>[...tables].some(table=>new RegExp('\\b'+table.replace('.','\\.')+'\\b','i').test(x.definition)));
const status={status:'SOURCE_CENSUS_NOT_NATIVE_EXECUTION_AUTHORITY',checkedAt:new Date().toISOString(),sourceCommit,fixtureFile,fixtureSha256:hash(fixture),helperManifestSha256:hash(manifestRaw),helperSha256:helperEntry.sha256,rootCount:roots.length,sourceOccurrences:occurrences,sourceInputCount:sourceInputs.size,sourceInputs:[...sourceInputs.values()],directFixtureEffects:effects,affectedTables:[...tables].sort(),attachedLiteralTriggers:attached,selectedTransitiveDefinitions:callees,unknownCalls:[...new Set(unknown)].sort(),dynamicTriggerDdl,dynamicFunctionDdl:functions.unresolved,selectedSchemaStatements:selectedSchema,limitations:'Literal function CREATE/DROP/RENAME semantics and literal trigger CREATE/DROP are traced. Conditional trigger/function DDL, unresolved builtins and actual target catalogue require independent review. Trigger selection conservatively includes every event for each potential written table. No source census substitutes for native compile, installed trigger identity, actual assertion outcomes or fresh rollback restoration.',independentReview:false,installedCatalogueVerified:false,nativeExecuted:false};
fs.writeFileSync(output,JSON.stringify(status,null,2)+'\n',{flag:'wx'});
console.log(JSON.stringify({status:status.status,roots:roots.length,inputs:sourceInputs.size,occurrences,affectedTables:tables.size,attachedTriggers:attached.length,transitiveDefinitions:callees.length,unknownCalls:status.unknownCalls,dynamicTriggerDdl:dynamicTriggerDdl.length,dynamicFunctionDdl:functions.unresolved.length,schemaStatements:selectedSchema.length,bytes:fs.statSync(output).size,sha256:hash(fs.readFileSync(output)),nativeExecuted:false}));
