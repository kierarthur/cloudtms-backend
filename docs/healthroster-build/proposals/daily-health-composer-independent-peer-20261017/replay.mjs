// Independent offline replay. Only this proposal receives outputs.
import fs from 'node:fs';
import path from 'node:path';
import cp from 'node:child_process';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const repo=path.resolve(here,'../../../..');
const author='D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026';
const root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const freeze='bc9f07844759bb3a914d6a48488dd25c2946489c';
const current='a321f8166310a114f60f31c15ff3aaa14fbae96f';
const folder='docs/healthroster-build/proposals/daily-health-composer-full-health-source-20261017';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=(commit,file,where=repo)=>{const r=cp.spawnSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',where,'show',commit+':'+file],{maxBuffer:16e6});assert.equal(r.status,0,r.stderr.toString());return r.stdout;};
const write=(file,value)=>{const target=path.join(here,file);fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,typeof value==='string'||Buffer.isBuffer(value)?value:JSON.stringify(value,null,2)+'\n');};
const read=f=>fs.readFileSync(path.join(here,'frozen',f));
const capture=JSON.parse(fs.readFileSync(path.join(here,'CAPTURE.json')));
const checks=[];
const check=(name,fn)=>{fn();checks.push({name,result:'PASS'});};
for(const row of capture.files)check('raw frozen saved/Git/captured equality '+row.file,()=>{const bytes=read(row.file);assert.equal(sha(bytes),row.sha256);assert(bytes.equals(git(freeze,folder+'/'+row.file)));assert(bytes.equals(fs.readFileSync(path.join(author,folder,row.file))));});
const proof=JSON.parse(read('SOURCE_PROOF.json'));
for(const row of proof.transfers)check('complete original raw transfer '+row.file,()=>assert(read(row.file).equals(git(row.parentCommit,row.parentPath))));
for(const row of proof.originalSixHealthAndNativeParents)check('original complete native parent '+row.file,()=>assert(read('reference/root/'+row.file).equals(git(row.commit,row.file))));
const reversal=JSON.parse(read('FORWARD_REVERSE.json'));
check('complete Review raw inverse two finite hunks',()=>{let candidate=read('templates/01_review.sql.tmpl').toString();assert.equal(sha(candidate),reversal.afterSha256);for(const h of reversal.hunks.toReversed()){assert.equal(candidate.split(h.after).length,2);candidate=candidate.replace(h.after,()=>h.before);}assert(Buffer.from(candidate).equals(git(reversal.parentCommit,reversal.parentPath)));assert(Buffer.from(candidate).equals(read(reversal.wholeParent)));assert.equal(sha(candidate),reversal.beforeSha256);});
const nativeCurrent=[];
for(const row of proof.originalSixHealthAndNativeParents.filter(r=>!r.file.endsWith('migration-lock.json'))){const bytes=git(current,row.file);check('current a321 complete original retained '+row.file,()=>assert.equal(sha(bytes),row.sha256));nativeCurrent.push({file:row.file,bytes:bytes.length,sha256:sha(bytes),equalOriginal:true});}
for(const row of proof.invoiceOwners)check('actual current complete0955 owner '+row.signature,()=>{const whole=git(current,row.wholeSourceFile);assert.equal(sha(whole),row.wholeSourceSha256);assert(whole.includes(read(row.file)));assert.equal(sha(read(row.file)),row.bodySha256);});
const priorPeer='docs/healthroster-build/proposals/daily-health-fixture-context-independent-peer-20261017';
const producerPins=JSON.parse(git('2b86412de4cc74a9bd6b31aa9c7dc336c067e6d5',priorPeer+'/PRODUCER_PINS.json'));
const index=git(current,'broker/src/index.js');
for(const row of producerPins.functions)check('current genuine Health producer retained '+row.name,()=>{const body=git('2b86412de4cc74a9bd6b31aa9c7dc336c067e6d5',priorPeer+'/'+row.sourceOutput);assert.equal(sha(body),row.sha256);assert(index.includes(body));assert.equal(index.toString().split(body.toString()).length,2);});
const workerFiles=['broker/src/daily-rate-correction.js','broker/src/daily-canonical-authority.js','broker/src/contract-correction-v3.mjs','tests/daily-rate-http.test.mjs','tests/daily-rate-calculator.test.mjs'];
const workerPins=[];
for(const file of workerFiles){const bytes=git(current,file);check('actual current Worker/module/test byte equality '+file,()=>assert(bytes.equals(fs.readFileSync(path.join(root,file)))));write('current-health/'+file,bytes);workerPins.push({file,commit:current,bytes:bytes.length,sha256:sha(bytes)});}
write('current-health/package.json','{"private":true,"type":"module"}\n');
const run=(name,args)=>{const start=Date.now();const result=cp.spawnSync(process.execPath,args,{cwd:repo,encoding:'utf8',maxBuffer:8e6});write('replay/'+name+'.stdout.txt',result.stdout);write('replay/'+name+'.stderr.txt',result.stderr);assert.equal(result.status,0,result.stdout+result.stderr);return {name,status:'PASS',exitCode:result.status,elapsedMs:Date.now()-start};};
const runs=[];
runs.push(run('author-source16',['--test',path.join(here,'frozen/source.test.mjs')]));
runs.push(run('actual-current-worker21',['--test',path.join(here,'current-health/tests/daily-rate-http.test.mjs'),path.join(here,'current-health/tests/daily-rate-calculator.test.mjs')]));
const sqlInputs=['health/00_presentation.sqlinc','health/05_review_enrollment.sqlinc','health/10_revision_apply.sqlinc','health/99_acl.sqlinc','admission/ONE_TIME_ADMISSION.sqlinc','verification/mandatory_security.sql','ADDITIVE_REVERSE.sqlinc','grammar-source.py'];
for(const file of sqlInputs){const raw=read(file);write('grammar-input/'+file,raw);check('unchanged grammar input '+file,()=>assert(raw.equals(fs.readFileSync(path.join(here,'grammar-input',file)))));}
const python='C:/Users/KierArthur/AppData/Local/Programs/Python/Python312/python.exe';
const pyFile='//?/'+path.join(here,'grammar-input/grammar-source.py').replaceAll('\\','/');
const grammar=cp.spawnSync(python,[pyFile],{cwd:repo,encoding:'utf8',maxBuffer:4e6});assert.equal(grammar.status,0,grammar.stdout+grammar.stderr);write('replay/grammar.stdout.txt',grammar.stdout);write('replay/grammar.stderr.txt',grammar.stderr);
const gateFile='scripts/bpay-next-source-gates.mjs';const gateBytes=git('77b70db4c9c19eab2a473a6600f438fb3c5acc01',gateFile);check('actual raw census gate retained in a321',()=>assert(gateBytes.equals(git(current,gateFile))));
const metadata=[];
for(const file of ['docs/bpay-next/final-definer-closures.json','supabase/release/current-release.json','supabase/release/migration-lock.json']){const bytes=git(current,file);check('current metadata saved/Git equality '+file,()=>assert(bytes.equals(fs.readFileSync(path.join(root,file)))));metadata.push({file,commit:current,bytes:bytes.length,sha256:sha(bytes),mountPayload:false});}
const registry=JSON.parse(git(current,'docs/bpay-next/final-definer-closures.json'));
check('eight composer definitions have no live chain',()=>{for(const sig of proof.newSignatures)assert(!registry.chains.some(x=>x.signature===sig));});
const role='supabase/repeatable/17102026_0910_nhsp_v6_daily_role_identity/01_complete_context.sqlinc';
const roleBytes=git(current,role);const roleProof=JSON.parse(git('1d00a57a1cf292f2a0c6cf8c17898d1e13204d31','docs/healthroster-build/proposals/daily-health-role-identity-forward-20261017/SOURCE_PROOF.json'));
check('current role successor is reviewed exact complete leaf',()=>assert(roleBytes.equals(git('1d00a57a1cf292f2a0c6cf8c17898d1e13204d31','docs/healthroster-build/proposals/daily-health-role-identity-forward-20261017/payload/'+roleProof.forwardLeaf))));
write('CURRENT_BINDING.json',{current,completeNativeParents:nativeCurrent,workerPins,metadata,reviewedRoleLeaf:{file:role,bytes:roleBytes.length,sha256:sha(roleBytes)},sevenGenuineHealthProducersUnchanged:true,bankBodiesSelected:0,native:'NOT_RUN'});
write('SOURCE_CHECK_RESULTS.json',{checks:checks.length,results:checks,additionalUnreportedFindings:0,knownHeldScalarMappingDefects:1,qualifiedRuntime:false,native:'NOT_RUN',rootWrites:0,OfficeWrites:0,bankBodySemanticReads:0});
write('REPLAY_RESULTS.json',{runs,sourceTests:16,negativeMutationCases:5,currentWorkerTransportTests:12,currentCanonicalCalculatorTests:9,grammarInputs:7,closedPlpgsqlHelpers:5,heldTemplatesGrammar:'NOT_PARSED_UNPROVIDED_BANK_SLOTS',native:'NOT_RUN'});
console.log(JSON.stringify({checks:checks.length,runs:2,tests:37,grammarInputs:7,native:'NOT_RUN'}));
