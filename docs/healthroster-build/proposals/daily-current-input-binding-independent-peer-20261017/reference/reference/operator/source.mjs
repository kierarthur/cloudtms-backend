import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
import {ROOT,PRIVATE_STATE,check,sha} from './contract.mjs';
import {git,physical,reviewedImage,preflight} from './git-image-batch.mjs';
export {git,physical};
export const here=path.dirname(fileURLToPath(import.meta.url));
const same=(root,file,commit)=>{const b=fs.readFileSync(root+'/'+file);check(b.equals(git(root,'show',commit+':'+file))&&b.equals(git(root,'show',':'+file)),'FIXTURE_CURRENT_COMMITTED_SOURCE_REQUIRED');return b;};
export function assertReviewedImage(root,reviewedCommit,files){return reviewedImage(root,reviewedCommit,files);}
export function assertReviewedExecutionSource(reviewedCommit){
 // A required root setup/evidence commit may advance HEAD. Its source image may not change.
 sourcePreflight();const top=git(here,'rev-parse','--show-toplevel').toString().trim();
 const relative=path.relative(top,here).replaceAll('\\','/'),manifest=JSON.parse(same(top,relative+'/MANIFEST.json',reviewedCommit));
 return assertReviewedImage(top,reviewedCommit,[relative+'/MANIFEST.json',...manifest.files.map(r=>relative+'/'+r.file)]);
}
export function sourcePreflight(){return preflight({here,ROOT,PRIVATE_STATE,check,sha});}
export function committedRootFile(file){
 check(typeof file==='string'&&/^docs\/healthroster-build\/[A-Za-z0-9_./-]+$/.test(file)&&!file.split('/').includes('..'),'FIXTURE_ROOT_EVIDENCE_PATH_REQUIRED');
 return same(ROOT,file,git(ROOT,'rev-parse','HEAD').toString().trim());
}
export function ownedDirectory(value){
 check(typeof value==='string'&&path.isAbsolute(value),'FIXTURE_OWNED_PATH_REQUIRED');const p=path.resolve(value).replaceAll('\\','/');
 check(path.posix.dirname(p)===ROOT+'/.codex-tmp'&&/^daily-genuine-stages-20261017-[a-f0-9]{32}$/.test(path.posix.basename(p)),'FIXTURE_OWNED_PATH_REQUIRED');
 check(physical(path.dirname(p))===ROOT+'/.codex-tmp','FIXTURE_PRIVATE_PARENT_REQUIRED');return p;
}
