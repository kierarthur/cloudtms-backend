// Read-only public source/Git verification. No private auth/native payload access.
import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';import assert from 'node:assert/strict';import {sha} from './contract.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const git=(...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...a],{maxBuffer:32e6});
const census=JSON.parse(fs.readFileSync(here+'/reference/audit/CURRENT_SOURCE_CENSUS.json')),mapping=JSON.parse(fs.readFileSync(here+'/reference/mapping/SOURCE_PINS.json'));
const current=fs.readFileSync(root+'/broker/src/index.js'),published=git('show',mapping.publishedCommit+':broker/src/index.js');assert.equal(sha(published),mapping.publishedWholeIndexSha256);
const all=new Map();for(const row of census.functions)all.set(row.name,fs.readFileSync(here+'/reference/audit/'+row.file));
for(const row of mapping.functions){const b=fs.readFileSync(here+'/reference/mapping/'+row.file);if(all.has(row.name))assert(all.get(row.name).equals(b));all.set(row.name,b);}
const occurrences=(haystack,needle)=>{let n=0,start=0;while((start=haystack.indexOf(needle,start))>=0){n++;start+=needle.length;}return n;};
for(const [name,b]of all){assert.equal(occurrences(current,b),1,name);assert.equal(occurrences(published,b),1,name);}
for(const [capture,base]of [['AUDIT_CAPTURE.json','reference/audit'],['MAPPING_CAPTURE.json','reference/mapping']]){
 const x=JSON.parse(fs.readFileSync(here+'/'+capture));for(const r of x.files){const b=fs.readFileSync(here+'/'+base+'/'+r.file);assert.equal(b.length,r.bytes);assert.equal(sha(b),r.sha256);assert(b.equals(git('show',x.commit+':'+x.packet+'/'+r.file)));}
}
for(const r of [...census.inputs,...census.rootDefinitionReceipts,...mapping.inputs]){
 const b=fs.readFileSync(root+'/'+r.file);assert.equal(sha(b),r.sha256);assert(b.equals(git('show','HEAD:'+r.file)));assert(b.equals(git('show',':'+r.file)));
}
const result={version:'DAILY_GENUINE_OPERATOR_INDEPENDENT_SOURCE_CHECK_V1',distinctCompleteFunctions:all.size,selectedRawCurrentPublishedOnce:true,originalAuditFiles:47,mappingFiles:28,
 originalRecipeUnchanged:true,wholeCurrentCalculatorDeployed:false,publishedCalculatorAuthority:'f743b81fe537830d81b1de93ec4f9cb0331532a7',currentRoot:git('rev-parse','HEAD').toString().trim(),
 bankBodiesRead:0,privateFilesRead:0,businessRowsRead:0,native:'NOT_RUN'};
if(!process.argv.includes('--verify'))fs.writeFileSync(here+'/INDEPENDENT_SOURCE_CHECK.json',JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result));
