// Independent immutable constructor/raw/lineage review. No SQL or generators.
import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';import test from 'node:test';import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const constructorCommit='aad0dd848253abca76a4917765b8d20c4d128646',lineageCommit='a52538b17e234d76ac3cfc5f43165a3bfaa103e1';
const core='docs/healthroster-build/proposals/a0-source-selector-domain-construction-20261014/',tuple='docs/healthroster-build/proposals/a0-source-selector-tuples-forward-20261014/';
const footprint='docs/healthroster-build/proposals/a0-source-native-footprint-construction-20261015/',rawBase='docs/healthroster-build/proposals/a0-source-raw-selector-guard-construction-20261015/',lineage='docs/healthroster-build/proposals/a0-source-lineage-selector-successor-20261015/';
const sha=x=>crypto.createHash('sha256').update(x).digest('hex');
const git=(rev,file)=>execFileSync('git',['-c','maintenance.auto=false','cat-file','blob',rev+':'+file],{cwd:repo,maxBuffer:32e6});
const blobs=new Map(),pins=[],originalGit=new Map(),seen=new Set();
const capture=(rev,file)=>{const key=rev+':'+file;if(seen.has(key))return originalGit.get(key);const b=git(rev,file);seen.add(key);originalGit.set(key,b);blobs.set(path.join(repo,file),b);pins.push({commit:rev,file,bytes:b.length,sha256:sha(b)});return b;};
const list=(rev,args)=>execFileSync('git',args,{cwd:repo,encoding:'utf8'}).trim().split('\n').filter(Boolean);
for(const commit of [constructorCommit,lineageCommit])for(const f of list(commit,['diff-tree','--no-commit-id','--name-only','-r',commit]))capture(commit,f);
for(const base of [core,tuple])for(const f of list(constructorCommit,['ls-tree','-r','--name-only',constructorCommit,'--',base]))capture(constructorCommit,f);
const scannerBytes=capture(constructorCommit,'scripts/cloudtms-managed-release-sql.mjs'),{scanManagedSql}=await import('data:text/javascript;base64,'+scannerBytes.toString('base64'));
const json=f=>JSON.parse(blobs.get(path.join(repo,f)));
for(const base of [core,tuple,lineage])for(const r of json(base+'COMPLETE_CURRENT_REVERSES.json').records)capture(r.source_commit,r.source_path);
for(const base of [footprint,rawBase])for(const p of json(base+'INPUT_PINS.json').pins){const b=capture(p.source_commit,p.source_path);assert.equal(sha(b),p.sha256);assert.ok(b.equals(blobs.get(path.join(repo,base+p.file))));}
const e=json(rawBase+'RAW_COLUMN_SOURCE_EVIDENCE.json'),mapping=json(rawBase+'reference/SOURCE_RAW_ID_DDL_MAPPING.json');
assert.equal(e.required_relations,38);assert.equal(e.raw_column_evidence.length,75);assert.equal(mapping.source_pins.length,22);
for(const p of mapping.source_pins){const b=capture(e.source_commit,p.path);assert.equal(sha(b),p.raw_sha256);}
const rulesBytes=capture(e.source_commit,e.additional_current_rules_schema.path);assert.equal(sha(rulesBytes),e.additional_current_rules_schema.sha256);
const rulesParsed=scanManagedSql(rulesBytes.toString());const rules=rulesParsed.statements.map(s=>rulesParsed.source.slice(s.start,s.end));
const verifiedColumns=[];
for(const col of e.raw_column_evidence){const table=mapping.tables.find(t=>t.relation===col.relation);for(const d of col.declarations){
 const statements=table?table.ddl.filter(x=>x.source_path===d.source_path).map(x=>x.statement):rules;
 const statement=statements.find(s=>sha(s)===d.statement_raw_sha256);assert.ok(statement,col.relation+'.'+col.column+' statement');
 const full=capture(e.source_commit,d.source_path).toString();assert.ok(full.includes(statement),col.relation+'.'+col.column+' literal source');
 assert.match(statement,new RegExp('(?:^|[\\s,])"?'+col.column+'"?\\s+(?:pg_catalog\\.)?'+col.type+'\\b','i'));}
 verifiedColumns.push({relation:col.relation,column:col.column,type:col.type});}
const le=json(lineage+'LINEAGE_PHYSICAL_COLUMN_EVIDENCE.json'),contractBytes=capture(le.source_commit,le.source_path);assert.equal(sha(contractBytes),le.source_sha256);
const contract=JSON.parse(contractBytes),relation=contract.relations.find(r=>r.schema+'.'+r.name===le.relation);assert.ok(relation);
for(const col of le.columns)assert.deepEqual(relation.columns.find(c=>c.name===col.name),col);
for(const base of [core,tuple,footprint,rawBase,lineage])for(const p of json(base+'SOURCE_MANIFEST.json').artifacts){const b=blobs.get(path.join(repo,base+p.file));assert.ok(b,p.file);assert.equal(b.length,p.bytes);assert.equal(sha(b),p.sha256);}
for(const root of ['docs/healthroster-build/root-a0-source-footprint-source-20261015/','docs/healthroster-build/root-a0-source-lineage-source-20261015/'])for(const p of json(root+'SHA256_MANIFEST.json').artifacts){const b=blobs.get(path.join(repo,root+p.file));assert.ok(b,p.file);assert.equal(b.length,p.bytes);assert.equal(sha(b),p.sha256);}
const local=p=>p instanceof URL?fileURLToPath(p):String(p);
const frozenFs={readFileSync(p,encoding){const b=blobs.get(path.resolve(local(p)));assert.ok(b,'Unpinned source read: '+local(p));return encoding?b.toString(encoding):Buffer.from(b);},
 readdirSync(p){const dir=path.resolve(local(p));assert.ok([core,tuple,lineage].some(b=>dir===path.join(repo,b,'forward')));return [...blobs.keys()].filter(f=>path.dirname(f)===dir).map(f=>path.basename(f));}};
const frozenExec=(command,args)=>{assert.equal(command,'git');assert.equal(args[0],'cat-file');assert.equal(args[1],'blob');assert.ok(originalGit.has(args[2]),'Unpinned git read '+args[2]);return Buffer.from(originalGit.get(args[2]));};
const adaptations=[];
for(const file of ['tests/healthroster-a0-source-selector-domain.test.mjs','tests/healthroster-a0-source-selector-tuples.test.mjs','tests/healthroster-a0-source-native-footprint.test.mjs','tests/healthroster-a0-source-raw-selectors.test.mjs','tests/healthroster-a0-source-lineage-selectors.test.mjs']){
 const commit=file.endsWith('lineage-selectors.test.mjs')?lineageCommit:constructorCommit,original=blobs.get(path.join(repo,file))??capture(commit,file);
 let source=original.toString().replace(/^import .*;\r?\n/gm,'');source=source.replace('import.meta.url',JSON.stringify(new URL('file:///'+path.join(repo,file).replaceAll('\\','/')).href));assert.ok(!source.includes('import.meta'));
 new vm.Script(source,{filename:commit+':'+file}).runInContext(vm.createContext({fs:frozenFs,cp:{execFileSync:frozenExec},execFileSync:frozenExec,scanManagedSql,test,assert,createHash:crypto.createHash,URL,Buffer,console}));
 adaptations.push({file,commit,original_sha256:sha(original),harness_source_sha256:sha(source),changes:'Only ESM imports and import.meta URL injected; all original test bodies and assertions unchanged.'});}
const lexical=[];
for(const base of [footprint,rawBase,lineage])for(const [file,b] of blobs)if(file.startsWith(path.join(repo,base))&&file.endsWith('.sqlinc')&&(file.includes(path.sep+'construction'+path.sep)||file.includes(path.sep+'forward'+path.sep))){const p=scanManagedSql(b.toString());assert.equal(p.meta.length,0);lexical.push({file:path.relative(repo,file).replaceAll('\\','/'),statements:p.statements.length,sha256:sha(b)});}
assert.equal(lexical.filter(p=>p.file.startsWith(lineage)).reduce((n,p)=>n+p.statements,0),40);
fs.writeFileSync(path.join(here,'FROZEN_REVIEW_INPUT_PINS.json'),JSON.stringify({status:'BOUNDED_INDEPENDENT_SOURCE_CONSTRUCTOR_RAW_LINEAGE_REVIEW',constructorCommit,lineageCommit,
 SQL_executed:false,native:false,generators_executed:false,another_lane_edited:false,pins,verifiedColumns,lineageColumns:le.columns,lexical,adaptations},null,2)+'\n');
