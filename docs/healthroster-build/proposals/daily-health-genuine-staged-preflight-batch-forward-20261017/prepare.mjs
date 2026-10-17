// Finite source-only proposal. Never writes the original package or any root runtime target.
import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';import {sha} from '../daily-health-genuine-staged-operator-20261017/contract.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),base=path.resolve(here,'../daily-health-genuine-staged-operator-20261017');
const original=fs.readFileSync(base+'/source.mjs'),manifestBytes=fs.readFileSync(base+'/MANIFEST.json');
if(sha(manifestBytes)!=='0a908ee36e34c41457b797a7609676c46373c89d924f66846100c87a2dc47f0d')throw Error('ORIGINAL_MANIFEST_CHANGED');
fs.mkdirSync(here+'/reference',{recursive:true});fs.mkdirSync(here+'/payload',{recursive:true});
for(const name of ['source.mjs','MANIFEST.json','source-image.test.mjs']){const file=here+'/reference/'+name,bytes=fs.readFileSync(base+'/'+name);if(fs.existsSync(file)){if(!bytes.equals(fs.readFileSync(file)))throw Error('COMPLETE_PARENT_CHANGED');}else fs.writeFileSync(file,bytes,{flag:'wx'});}
const old=original.toString();const begin=old.indexOf('export function assertReviewedImage('),middle=old.indexOf('export function assertReviewedExecutionSource('),preflight=old.indexOf('export function sourcePreflight(){'),after=old.indexOf('export function committedRootFile(');
if(!(begin>0&&middle>begin&&preflight>middle&&after>preflight))throw Error('COMPLETE_PARENT_LAYOUT_CHANGED');
const source="import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';\nimport {ROOT,PRIVATE_STATE,check,sha} from './contract.mjs';\nimport {git,physical,reviewedImage,preflight} from './git-image-batch.mjs';\nexport {git,physical};\nexport const here=path.dirname(fileURLToPath(import.meta.url));\n"
 +old.slice(old.indexOf('const same='),begin)
 +"export function assertReviewedImage(root,reviewedCommit,files){return reviewedImage(root,reviewedCommit,files);}\n"
 +old.slice(middle,preflight)
 +"export function sourcePreflight(){return preflight({here,ROOT,PRIVATE_STATE,check,sha});}\n"
 +old.slice(after);
const helper=fs.readFileSync(here+'/git-image-batch.mjs'),manifest=JSON.parse(manifestBytes);
manifest.files=manifest.files.map(r=>r.file==='source.mjs'?{file:r.file,bytes:Buffer.byteLength(source),sha256:sha(source)}:r);
manifest.files.push({file:'git-image-batch.mjs',bytes:helper.length,sha256:sha(helper)});
manifest.successor={version:'DAILY_GENUINE_BATCH_IMAGE_FORWARD_V1',parentCommit:'163646cb752fc64d4221ac70b685ecd952afe7b0',parentManifestSha256:sha(manifestBytes),unchangedBusinessNativeFreshnessJournalActorControls:true};
fs.writeFileSync(here+'/payload/source.mjs',source);fs.writeFileSync(here+'/payload/git-image-batch.mjs',helper);fs.writeFileSync(here+'/payload/MANIFEST.json',JSON.stringify(manifest,null,2)+'\n');
const test=fs.readFileSync(base+'/source-image.test.mjs','utf8'),line="import {assertReviewedImage} from './source.mjs';";
if(test.split(line).length!==2)throw Error('EXACT_ORIGINAL_TEST_IMPORT_CHANGED');
fs.writeFileSync(here+'/batch-image.test.mjs',test.replace(line,"import {reviewedImage as assertReviewedImage} from './git-image-batch.mjs';").replace("from './contract.mjs'","from '../daily-health-genuine-staged-operator-20261017/contract.mjs'").replace("from './stages.mjs'","from '../daily-health-genuine-staged-operator-20261017/stages.mjs'"));
console.log(JSON.stringify({payloads:3,originalPackageChanged:false,target:'daily-health-genuine-staged-operator-20261017',originalSourceSha256:sha(original),targetSourceSha256:sha(source),targetManifestSha256:sha(JSON.stringify(manifest,null,2)+'\n')}));
