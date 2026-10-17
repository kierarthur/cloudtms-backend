// Pure finite source recipe. Future R8 authority must come from a genuine committed public receipt.
import fs from 'node:fs';import crypto from 'node:crypto';
const here=new URL('./',import.meta.url),read=f=>fs.readFileSync(new URL('reference/'+f,here),'utf8');
export const hash=b=>crypto.createHash('sha256').update(b).digest('hex'),json=v=>JSON.stringify(v,null,2)+'\n';
export const baseCommit='4d5873f820b764681d5fc36ec689cde6e5882ee8';
export const ROOT='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
export const receiptFile='docs/healthroster-build/v6-owner-delivery-20261016/TEST_AUTH_REFRESH_R8_SUCCESS_20261017.json';
export const folders=Object.freeze({operator:'docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/',holder:'docs/healthroster-build/proposals/daily-health-s0-catalogue-holder-20261017/',coordinator:'docs/healthroster-build/proposals/daily-health-s0-stage-coordinator-20261017/'});
export const exactCommit=v=>{if(!/^[0-9a-f]{40}$/.test(v??''))throw Error('R8_GENUINE_COMMIT_REQUIRED');return v;};
export function validateBinding(binding){
 if(!binding||Object.keys(binding).sort().join(',')!=='observation,publicReceipt')throw Error('R8_CLOSED_PUBLIC_BINDING_REQUIRED');
 const {observation,publicReceipt}=binding;
 if(!observation||Object.keys(observation).sort().join(',')!=='commit,file,sha256'||observation.file!==receiptFile||!/^[0-9a-f]{64}$/.test(observation.sha256??'')||typeof publicReceipt!=='string'||Buffer.byteLength(publicReceipt)>32768)throw Error('R8_PUBLIC_RECEIPT_REQUIRED');
 exactCommit(observation.commit);if(hash(publicReceipt)!==observation.sha256)throw Error('R8_PUBLIC_RECEIPT_HASH_REQUIRED');
 const r=JSON.parse(publicReceipt),a=r?.actualClosedResult,d=a?.diagnostic?.admission;
 if(r.version!=='TEST_AUTH_REFRESH_R8_ACTUAL_SUCCESS_V1'||a?.status!=='AUTH_REFRESHED'||a.code!=='AUTH_REFRESHED'||a.browserClosed!==true||a.loginRequests!==1||!Number.isInteger(a.refreshRequests)||a.refreshRequests<0||a.refreshRequests>1||a.forbiddenWriteRequests!==0||a.refusedRequests!==0||!Number.isInteger(a.suppressedStartupRequests)||a.suppressedStartupRequests<0||a.suppressedStartupRequests>1
  ||d?.meStatus!==200||d.actorUuid!==true||d.admin!==true||d.workspaceStatus!==200||d.contract!==true||d.queries!==true
  ||r.privateContentsReadOrHashedByParent!==false||r.sharedFrontendStateUntouched!==true||r.browserAndChildClosed!==true||r.processExitCode!==0||r.nativeIntervalActive!==false||r.startupPingForwarded!==false||r.startupResponseFabricated!==false||r.businessFixturesCreated!==false
  ||r.qualification!=='ME_ADMIN_AND_QUERY_WORKSPACE_ONLY_NOT_NATIVE_WPPP_OR_BUSINESS_CURRENTNESS')throw Error('R8_ACTUAL_CLOSED_AUTH_SUCCESS_REQUIRED');
 if(typeof r.privateStatePath!=='string'||!new RegExp('^'+ROOT.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')+'/.codex-tmp/test-auth-refresh-20261017-[a-f0-9]{32}/storage-state\\.json$').test(r.privateStatePath)
  ||r.privateStatePath===JSON.parse(read('R7_PUBLIC_RECEIPT.json')).privateStatePath)throw Error('R8_FRESH_PUBLIC_STATE_PATH_REQUIRED');
 return {observation:{...observation},privateStatePath:r.privateStatePath}; // Path literal only; never inspect its target.
}
function rowUpdate(before,changes){const next=JSON.parse(before),beforeRows=[],afterRows=[];for(const[file,patch]of changes){const row=next.files.find(r=>r.file===file);if(!row)throw Error('R8_EXACT_PARENT_MANIFEST_ROW_REQUIRED');beforeRows.push({...row});Object.assign(row,patch);afterRows.push({...row});}const reverse=structuredClone(next);for(const row of beforeRows)reverse.files[reverse.files.findIndex(r=>r.file===row.file)]=row;if(json(reverse)!==before)throw Error('R8_WHOLE_MANIFEST_REVERSE_FAILED');return {text:json(next),beforeRows,afterRows,retainedRows:next.files.length-changes.length,wholeRawInverse:true};}
function result(phase,images,proof){return {phase,targets:Object.entries(images).map(([name,text])=>({target:folders[phase]+name,file:name,beforeSha256:hash(read(phase+'/'+name)),afterSha256:hash(text),bytes:Buffer.byteLength(text),text})),proof};}
export function holderBody(){const text=fs.readFileSync(new URL('payload/holder/source.mjs',here),'utf8'),proof=JSON.parse(fs.readFileSync(new URL('HOLDER_FULL_PARENT_FORWARD.json',here)));let reverse=text;for(const c of [...proof.changes].reverse())reverse=reverse.replace(c.after,c.before);if(reverse!==read('holder/source.mjs')||hash(text)!==proof.afterSha256)throw Error('R8_COMPLETE_HOLDER_BODY_REQUIRED');return {text,proof};}
export function operator(binding){
 const actual=validateBinding(binding),before=read('operator/contract.mjs'),oldPath=JSON.parse(read('R7_PUBLIC_RECEIPT.json')).privateStatePath.slice(ROOT.length),newPath=actual.privateStatePath.slice(ROOT.length);
 if(before.split(oldPath).length!==2)throw Error('R8_ONE_PUBLIC_STATE_PATH_REQUIRED');const contract=before.replace(oldPath,newPath);if(contract.replace(newPath,oldPath)!==before)throw Error('R8_FULL_CONTRACT_REVERSE_FAILED');
 const oldPins=read('operator/SOURCE_PINS.json'),pins=JSON.parse(oldPins),oldObservation={...pins.authObservation};pins.authObservation=actual.observation;const reverse=structuredClone(pins);reverse.authObservation=oldObservation;if(json(reverse)!==oldPins)throw Error('R8_FULL_OBSERVATION_REVERSE_FAILED');const pinText=json(pins),manifest=rowUpdate(read('operator/MANIFEST.json'),[['contract.mjs',{bytes:Buffer.byteLength(contract),sha256:hash(contract)}],['SOURCE_PINS.json',{bytes:Buffer.byteLength(pinText),sha256:hash(pinText)}]]);
 return result('operator',{'contract.mjs':contract,'SOURCE_PINS.json':pinText,'MANIFEST.json':manifest.text},{contract:{oldPath,newPath,wholeRawInverse:true},observation:{before:oldObservation,after:actual.observation,wholeRawInverse:true},manifest});
}
export function holder(binding,operatorCommit){
 exactCommit(operatorCommit);const op=operator(binding),contract=op.targets.find(r=>r.file==='contract.mjs'),oldPins=read('holder/EXTERNAL_SOURCE_PINS.json'),pins=JSON.parse(oldPins),row=pins.find(r=>r.file===folders.operator+'contract.mjs');if(!row)throw Error('R8_HOLDER_CONTRACT_ROW_REQUIRED');const beforeRow={...row};Object.assign(row,{commit:operatorCommit,bytes:contract.bytes,sha256:contract.afterSha256});const reverse=structuredClone(pins);reverse[reverse.findIndex(r=>r.file===row.file)]=beforeRow;if(json(reverse)!==oldPins)throw Error('R8_HOLDER_FULL_REVERSE_FAILED');const text=json(pins),body=holderBody(),manifest=rowUpdate(read('holder/MANIFEST.json'),[['source.mjs',{bytes:Buffer.byteLength(body.text),sha256:hash(body.text)}],['EXTERNAL_SOURCE_PINS.json',{bytes:Buffer.byteLength(text),sha256:hash(text)}]]);
 return result('holder',{'source.mjs':body.text,'EXTERNAL_SOURCE_PINS.json':text,'MANIFEST.json':manifest.text},{operatorCommit,body:body.proof,pins:{beforeRows:[beforeRow],afterRows:[{...row}],retainedRows:pins.length-1,wholeRawInverse:true},manifest});
}
export function coordinator(binding,operatorCommit,holderCommit){
 exactCommit(operatorCommit);exactCommit(holderCommit);const op=operator(binding),held=holder(binding,operatorCommit),oldPins=read('coordinator/SOURCE_PINS.json'),pins=JSON.parse(oldPins),beforeRows=[],afterRows=[];
 for(const[image,commit]of [[op,operatorCommit],[held,holderCommit]])for(const t of image.targets){const row=pins.find(r=>r.file===t.target);if(!row)throw Error('R8_COORDINATOR_PIN_ROW_REQUIRED');beforeRows.push({...row});Object.assign(row,{commit,bytes:t.bytes,sha256:t.afterSha256});afterRows.push({...row});}
 const reverse=structuredClone(pins);for(const row of beforeRows)reverse[reverse.findIndex(r=>r.file===row.file)]=row;if(json(reverse)!==oldPins)throw Error('R8_COORDINATOR_FULL_REVERSE_FAILED');const text=json(pins),manifest=rowUpdate(read('coordinator/MANIFEST.json'),[['SOURCE_PINS.json',{bytes:Buffer.byteLength(text),sha256:hash(text)}]]);
 return result('coordinator',{'SOURCE_PINS.json':text,'MANIFEST.json':manifest.text},{operatorCommit,holderCommit,pins:{beforeRows,afterRows,retainedRows:pins.length-beforeRows.length,wholeRawInverse:true},manifest});
}
