import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),prior=path.resolve(dir,'../install2-office-correction-read-consumer-20261014');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
if(fs.existsSync(path.join(dir,'REVIEW_MANIFEST.json')))throw Error('FROZEN_SESSION_SUCCESSOR_DO_NOT_OVERWRITE');
const manifestBytes=fs.readFileSync(path.join(prior,'REVIEW_MANIFEST.json'));
if(sha(manifestBytes)!=='2d39c34ce67e3255b362cd9c8427fec4700b202790f624bbb4b6c044ce5354d6')throw Error('FROZEN14_MANIFEST_CHANGED');
const manifest=JSON.parse(manifestBytes);
for(const row of manifest.entries)if(sha(fs.readFileSync(path.join(prior,row.path)))!==row.sha256)throw Error(`FROZEN14_ARTIFACT_CHANGED:${row.path}`);
const old=JSON.parse(fs.readFileSync(path.join(prior,'CURRENT_OFFICE_READ_FULL_REVERSE.json')));
const originalMain=old.changed_current_owners.find(row=>row.path==='js/main.js').hunks[0];
const changes=[
 {before:'  const currentUser = SESSION?.user;',after:'  const sessionAtRead = SESSION, currentUser = sessionAtRead?.user;'},
 {before:'  const payFlags = [currentUser?.role, currentUser?.is_active, currentUser?.payment_authoriser, currentUser?.payment_golden_key];',
  after:'  const rolesAtRead = JSON.stringify(Array.isArray(currentUser?.roles) ? currentUser.roles : null);\r\n  const payFlags = [currentUser?.role, currentUser?.is_active, currentUser?.payment_authoriser, currentUser?.payment_golden_key];'},
 {before:'  const current = () => root.isConnected && SESSION?.user?.id === actor && isActiveTimesheetModalToken(token)',
  after:'  const current = () => root.isConnected && SESSION === sessionAtRead && SESSION?.user === currentUser\r\n    && SESSION?.user?.id === actor && isActiveTimesheetModalToken(token)\r\n    && JSON.stringify(Array.isArray(SESSION?.user?.roles) ? SESSION.user.roles : null) === rolesAtRead'},
];
let finalHunk=originalMain.after;
for(const h of changes){if(finalHunk.split(h.before).length!==2)throw Error('SESSION_HUNK_NOT_UNIQUE');finalHunk=finalHunk.replace(h.before,()=>h.after);}
const finalOwners=[],successorOwners=[];
for(const row of old.changed_current_owners){const before=fs.readFileSync(path.join(prior,'before',row.path));
  const predecessor=fs.readFileSync(path.join(prior,'forward',row.path));let after=predecessor.toString('utf8');let nextHunks;
  if(row.path==='js/main.js'){if(after.split(originalMain.after).length!==2)throw Error('MAIN_MOUNT_NOT_UNIQUE');
    after=after.replace(originalMain.after,()=>finalHunk);nextHunks=changes;
  }else{const h={before:'healthroster=20261014-r2',after:'healthroster=20261014-r3'};
    if(after.split(h.before).length!==2)throw Error('MAIN_CACHE_MARKER_NOT_UNIQUE');after=after.replace(h.before,()=>h.after);nextHunks=[h];}
  const fullHunk=row.path==='js/main.js'?{before:originalMain.before,after:finalHunk}:
    {before:row.hunks[0].before,after:row.hunks[0].after.replace('healthroster=20261014-r2',()=> 'healthroster=20261014-r3')};
  if(after.replace(fullHunk.after,()=>fullHunk.before)!==before.toString())throw Error('FINAL_CURRENT_OFFICE_REVERSE_FAILED');
  for(const [folder,bytes]of [['before',before],['predecessor',predecessor],['forward',Buffer.from(after)]]){
    const dest=path.join(dir,folder,row.path);fs.mkdirSync(path.dirname(dest),{recursive:true});fs.writeFileSync(dest,bytes);}
  finalOwners.push({...row,after_sha256:sha(after),hunks:[fullHunk]});
  successorOwners.push({path:row.path,before_sha256:sha(predecessor),after_sha256:sha(after),hunks:nextHunks});
}
for(const rel of ['forward/js/timesheet-correction-office-context.js','CURRENT_ME_SOURCE.js.txt','CURRENT_BACKEND_READ_SOURCE_PINS.json']){
  const dest=path.join(dir,rel);fs.mkdirSync(path.dirname(dest),{recursive:true});fs.writeFileSync(dest,fs.readFileSync(path.join(prior,rel)));}
const result={...old,schema:'OFFICE_CORRECTION_READ_SESSION_SUCCESSOR_V1',changed_current_owners:finalOwners,
 predecessor_manifest_sha256:sha(manifestBytes),predecessor_source_checks_sha256:manifest.source_checks_sha256,
 predecessor_artifact_count:manifest.artifact_count,successor_changed_owners:successorOwners,
 current_roles_array_source_bound:true,session_identity_bound:true,private_server_authority_unchanged:true};
fs.writeFileSync(path.join(dir,'CURRENT_OFFICE_READ_FULL_REVERSE.json'),JSON.stringify(result,null,2)+'\n');
let tests=fs.readFileSync(path.join(prior,'office-correction-read.test.mjs'),'utf8');
tests=tests.replace("user={id:actor,role:'admin'}",()=>"user={id:actor,roles:['admin'],email_signature_html:null}");
tests=tests.replace('actual saved main hunk admits legacy admin session shape',()=> 'actual saved main hunk admits actual handleMe roles-array shape');
if(tests.includes("user={id:actor,role:'admin'}"))throw Error('ACTUAL_ME_HARNESS_NOT_CHANGED');
fs.writeFileSync(path.join(dir,'office-correction-read.test.mjs'),tests);
console.log(JSON.stringify({preserved14:true,whole_current_reverse:2,successor_hunks:4,owners:finalOwners.map(row=>({path:row.path,sha256:row.after_sha256}))}));
