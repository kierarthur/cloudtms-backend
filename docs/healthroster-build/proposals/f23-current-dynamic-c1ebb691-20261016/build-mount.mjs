// Creates reviewable full proposed replacements only in this proposal folder.
import fs from'node:fs';import path from'node:path';import assert from'node:assert/strict';import{createHash}from'node:crypto';import{execFileSync}from'node:child_process';import{fileURLToPath}from'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(dir,'../../../..'),root=path.resolve(own,'../healthroster-install2-14102026'),commit='c1ebb691feb6a11324060e0c28210e2bd735624a';
const hash=x=>createHash('sha256').update(x).digest('hex'),gate='scripts/bpay-next-source-gates.mjs',base=execFileSync('git',['show',`${commit}:${gate}`],{cwd:root,maxBuffer:10e6}).toString();
const importBefore="import {reviewedHealthAggregateHazards} from './healthroster-aggregate-type-review.mjs';",importAfter="import {reviewedHealthDynamicHazards} from './healthroster-dynamic-source-review.mjs';\n"+importBefore;
assert.equal(base.split(importBefore).length,2);
const line=base.split('\n').find(l=>l.includes('} else assertSpecialConstructs(source,file,')&&l.includes('reviewedHealthAggregateHazards('));assert(line);
const prefix='assertSpecialConstructs(source,file,',end=');assertFrozenAuthority',start=line.indexOf(prefix)+prefix.length,stop=line.indexOf(end,start);assert(stop>start);
const expression=line.slice(start,stop),after=line.slice(0,start)+'reviewedHealthDynamicHazards(source,file,'+expression+',specialConstructViolations(source,file),executableParts(source,file))'+line.slice(stop);
const proposed=base.replace(importBefore,importAfter).replace(line,after);assert.equal(proposed.replace(importAfter,importBefore).replace(after,line),base);
const files=[];function output(target,source){const dest=path.join(dir,'forward',target);fs.mkdirSync(path.dirname(dest),{recursive:true});fs.writeFileSync(dest,source);files.push({target,sha256:hash(source),bytes:Buffer.byteLength(source)});}
output(gate,proposed);output('scripts/healthroster-dynamic-source-review.mjs',fs.readFileSync(path.join(dir,'healthroster-dynamic-source-review.mjs')));
const proofBase='docs/healthroster-build/v6-owner-delivery-20261016/F23_DYNAMIC_SOURCE_REVIEW_20261016';
for(const name of ['CLASSIFICATION.json','REVIEW.json','PRIOR_SOURCE_REVIEW.json','AST.json','TEST_RESULTS.json'])output(proofBase+'/'+name,fs.readFileSync(path.join(dir,name)));
for(const folder of ['bodies','producers'])for(const name of fs.readdirSync(path.join(dir,folder)))output(proofBase+'/'+folder+'/'+name,fs.readFileSync(path.join(dir,folder,name)));
for(const name of ['EXTRACTION.json','INHERITED_PRODUCER.json','INHERITED_INVOICE_ISSUE_COMPLETE.sqlinc'])output(proofBase+'/'+name,fs.readFileSync(path.join(dir,name)));
const result={version:'HEALTHROSTER_F23_DYNAMIC_FINITE_MOUNT_PROPOSAL_V1',sourceCommit:commit,rootSoleIntegrator:true,scope:'Two exact sourcegate edits plus one finite helper and complete preserved source evidence; no root changes made by this lane.',
 edits:[{path:gate,beforeSha256:hash(base),afterSha256:hash(proposed),edits:[{before:importBefore,after:importAfter,count:1},{before:line,after,count:1}],reverseEquality:true}],files,
 refusal:'Any source or dependency drift requires a new independent finite review and reseal. Do not merely renew hashes or transplant across current gate edits. This c1ebb complete gate replacement must be reconstructed against newer root changes by the sole integrator.',
 exclusions:['No1430 prebody/companion admission. No0400 native-census admission.','No SQL/type/DDL/ACL/runtime/Bank edits.','No generic EXECUTE,format,dollar SQL,pg_temp,function-prefix or native-catalog waiver.','No native/financial/provider/role/API/rollback/PartB PASS.'],native:'NOT_RUN',databaseCalls:0};
fs.writeFileSync(path.join(dir,'MOUNT.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({forwardFiles:files.length,gateBefore:hash(base),gateAfter:hash(proposed),helper:files[1].sha256}));
