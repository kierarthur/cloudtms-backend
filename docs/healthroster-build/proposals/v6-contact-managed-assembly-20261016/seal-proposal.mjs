// Seals this proposal only; no DB, network, release or external workspace write.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const sha=value=>createHash('sha256').update(value).digest('hex');
const test=JSON.parse(fs.readFileSync(path.join(here,'TEST_RESULT.json'),'utf8'));
assert.equal(test.exitCode,0);assert.equal(test.tests,11);assert.equal(test.failed,0);
for(const [file,expected]of Object.entries(test.validatedInputs))assert.equal(sha(fs.readFileSync(path.join(here,file))),expected,'RETEST_REQUIRED '+file);
const assembly=JSON.parse(fs.readFileSync(path.join(here,'ASSEMBLY_MANIFEST.json'),'utf8'));
const files=[];
function visit(relative=''){
 for(const entry of fs.readdirSync(path.join(here,relative),{withFileTypes:true})){
  const file=path.posix.join(relative,entry.name);if(file==='SOURCE_MANIFEST.json')continue;
  if(entry.isDirectory()){visit(file);continue;}
  assert(entry.isFile(),'UNCLASSIFIED_PROPOSAL_ENTRY '+file);
  const bytes=fs.readFileSync(path.join(here,file));assert(!bytes.includes(Buffer.from('\r\n')),'LF_REQUIRED '+file);
  files.push({path:file,bytes:bytes.length,sha256:sha(bytes)});
 }
}
visit();files.sort((a,b)=>a.path.localeCompare(b.path));
const payload=files.filter(row=>row.path.startsWith('payload/'));
assert.equal(payload.length,assembly.payloads.length);
for(const row of assembly.payloads)assert.equal(payload.find(file=>file.path==='payload/'+row.path)?.sha256,row.sha256);
const manifest={version:'CONTACT_COMPLETE_MANAGED_PROPOSAL_SOURCE_MANIFEST_V1',status:'SOURCE_ONLY_NOT_NATIVE_OR_PART_B_ACCEPTANCE',
 producerCommit:assembly.producerCommit,reviewCommit:assembly.reviewCommit,rootPrerequisiteCheckpoint:assembly.rootPrerequisiteCheckpoint,
 counts:assembly.counts,payloadFiles:payload.length,files,excludesSelf:true,
 validation:{focusedSourceRuntimeTests:11,failures:0,nativeCompile:'NOT_RUN',nativeFirstUse:'NOT_RUN',effectiveAcl:'NOT_RUN',concurrency:'NOT_RUN',api:'NOT_RUN',provider:'NOT_RUN',partB:'NOT_RUN'},
 scope:{proposalOnly:true,rootIntegrator:'I2 sole owner',databaseActions:0,locks:false,docker:false,deployment:false,bankingOwnedPayloads:false,
  priorReview416e:'PRESERVED',priorInvoice1808:'PRESERVED',frozenProducerE466:'PRESERVED',unrelatedPreexistingUntracked:'PRESERVED'}};
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({files:files.length,payloadFiles:payload.length,manifestSha256:sha(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json'))),native:'NOT_RUN'}));
