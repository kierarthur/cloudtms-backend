// Independent immutable physical Contract identity source review. No SQL.
import fs from 'node:fs';import path from 'node:path';import vm from 'node:vm';import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';import test from 'node:test';import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const commit='19efb87a26158e53fb4023676aa9b19e117b0685',base='docs/healthroster-build/proposals/qualification-physical-contract-identity-successor-20261014/';
const sha=raw=>crypto.createHash('sha256').update(raw).digest('hex');
const git=(rev,file)=>execFileSync('git',['-c','maintenance.auto=false','cat-file','blob',rev+':'+file],{cwd:repo,maxBuffer:30e6});
const blobs=new Map(),pins=[];
const capture=(rev,file)=>{const raw=git(rev,file);blobs.set(path.join(repo,file),raw);pins.push({commit:rev,file,bytes:raw.length,sha256:sha(raw)});return raw;};
const files=execFileSync('git',['ls-tree','-r','--name-only',commit,'--',base],{cwd:repo,encoding:'utf8'}).trim().split('\n');
for(const file of files)capture(commit,file);
const indexFile='docs/healthroster-build/proposals/a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const indexBytes=capture(commit,indexFile),{functionEvents,sqlTokens}=await import('data:text/javascript;base64,'+indexBytes.toString('base64'));
const scannerBytes=capture(commit,'scripts/cloudtms-managed-release-sql.mjs'),{scanManagedSql}=await import('data:text/javascript;base64,'+scannerBytes.toString('base64'));
const proof=JSON.parse(blobs.get(path.join(repo,base+'COMPLETE_NATIVE_REVERSES.json')));
for(const p of proof.raw_input_pins){const raw=capture(p.commit,p.file);assert.equal(sha(raw),p.sha256);assert.ok(raw.equals(blobs.get(path.join(repo,base+p.saved))));}
for(const p of proof.proofs){const full=blobs.get(path.join(repo,p.file)).toString();
 const marker='create or replace function '+p.name+'(';assert.equal(full.split(marker).length,2);const begin=full.indexOf(marker),end=full.indexOf('$function$;',begin)+'$function$;'.length;
 const raw=Buffer.from(full.slice(begin,end)+'\n');assert.ok(raw.equals(blobs.get(path.join(repo,base+'before/'+p.output))));
 const old=functionEvents(raw.toString()).events[0],next=functionEvents(blobs.get(path.join(repo,base+'forward/'+p.output)).toString()).events[0];
 assert.equal(old.id,next.id);assert.equal(old.header,next.header);}
const contractBytes=capture(proof.commit,'supabase/release/current-contract.json'),contract=JSON.parse(contractBytes);assert.equal(sha(contractBytes),proof.current_contract_sha256);
const physical=contract.relations.find(r=>r.schema==='public'&&r.name==='timesheets'),contracts=contract.relations.find(r=>r.schema==='public'&&r.name==='contracts'),financial=contract.relations.find(r=>r.schema==='public'&&r.name==='timesheets_financials');
const names=r=>new Set(r.columns.map(c=>c.name));assert.ok(!names(physical).has('client_id')&&!names(physical).has('candidate_id'));
assert.ok(names(physical).has('contract_id'));for(const key of ['id','client_id','candidate_id'])assert.ok(names(contracts).has(key));
assert.ok(names(financial).has('client_id')&&names(financial).has('candidate_id'));
assert.ok(!names(physical).has('root_id')&&!names(contracts).has('root_id'));
const rawDDL=JSON.parse(blobs.get(path.join(repo,base+'input/RAW_TIMESHEETS_DDL.json')));
for(const d of rawDDL.physical.ddl){const raw=capture(proof.commit,d.source_path);assert.equal(sha(raw),d.source_raw_sha256,d.source_path);
 assert.equal(Buffer.byteLength(d.statement),d.statement_bytes);assert.equal(sha(d.statement),d.statement_raw_sha256);}
const frozenFs={readFileSync(file,encoding){const raw=blobs.get(path.resolve(String(file)));assert.ok(raw,'Unpinned read: '+file);return encoding?raw.toString(encoding):Buffer.from(raw);}};
const testFile=base+'physical-identity-source.test.mjs',raw=blobs.get(path.join(repo,testFile));
let source=raw.toString().replace(/^import.*;\r?\n/gm,'');source=source.replace('import.meta.url',JSON.stringify(new URL('file:///'+path.join(repo,testFile).replaceAll('\\','/')).href));
assert.ok(!source.includes('import.meta'));new vm.Script(source,{filename:commit+':'+testFile}).runInContext(vm.createContext({fs:frozenFs,path,
 assert,test,createHash:crypto.createHash,fileURLToPath,functionEvents,Buffer,URL,console}));
const manifest=JSON.parse(blobs.get(path.join(repo,base+'FROZEN_ARTIFACT_MANIFEST.json')));
for(const p of manifest.artifacts){const raw=blobs.get(path.join(repo,base+p.file));assert.equal(raw.length,p.bytes);assert.equal(sha(raw),p.sha256);}
const lexical=files.filter(f=>f.startsWith(base+'forward/')).map(file=>{const p=scanManagedSql(blobs.get(path.join(repo,file)).toString());assert.equal(p.meta.length,0);return {file,statements:p.statements.length};});
const syntax=[];for(const file of files.filter(f=>f.endsWith('.mjs'))){execFileSync(process.execPath,['--input-type=module','--check'],
 {input:blobs.get(path.join(repo,file)),cwd:repo});syntax.push(file);}
assert.equal(syntax.length,3);
// Tokenised physical alias checks are case-insensitive; genuine report/DTO
// aliases and financial Clients are separate native relations, not T0 fields.
const aliasChecks=[];
for(const p of proof.proofs){const bytes=blobs.get(path.join(repo,base+'forward/'+p.output)),r=functionEvents(bytes.toString()).events[0],tokens=sqlTokens(r.body);
 const illegal=[];for(let i=0;i<tokens.length-2;i++)if(['root','sheet'].includes(tokens[i].value)&&tokens[i+1].value==='.'&&['client_id','candidate_id'].includes(tokens[i+2].value))illegal.push(tokens[i].value+'.'+tokens[i+2].value);
 assert.deepEqual(illegal,[]);aliasChecks.push({owner:p.name,physical_alias_violations:illegal});}
fs.writeFileSync(path.join(here,'FROZEN_REVIEW_INPUT_PINS.json'),JSON.stringify({status:'BOUNDED_INDEPENDENT_PHYSICAL_CONTRACT_SOURCE_REVIEW',commit,
 SQL_executed:false,native:false,generators_executed:false,another_lane_edited:false,pins,lexical,syntax,aliasChecks,
 schema:{timesheets_columns:physical.columns.length,contracts_columns:contracts.columns.length,financial_columns:financial.columns.length,source_only:true},
 adaptation:{testFile,original_sha256:sha(raw),harness_source_sha256:sha(source),changes:'ESM built-ins and exact pinned functionEvents injected; import.meta URL fixed to immutable virtual Git source. All thirteen test bodies/assertions unchanged.'}},null,2)+'\n');
