import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';import {spawnSync,execFileSync} from 'node:child_process';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const current='37ddadc0a50721834c9e8a6c06becfc983f0dc32',broad='f7afa8f66133b5b58abadbe1c3654a1e80b475a0',i2='dc95782df4bf67b90407f5464a666766596a64ab';
const files=['broker/src/candidate-app-backend.js','broker/src/index.js','broker/src/weekly-source/routes.js','candidate-broker/src/candidate-broker.js'];
const git=args=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:own,encoding:null,maxBuffer:64*1024*1024});
if(git(['branch','--show-current']).toString().trim()!=='codex/healthroster-v2-report-build-13102026')throw Error('OWN_LANE_REQUIRED');
const common=git(['merge-base',current,broad]).toString().trim();if(!common.startsWith('e0a02bb5'))throw Error('COMMON_SOURCE_CHANGED');
const frozen='docs/healthroster-build/proposals/report-ab-protected-dispatch/SOURCE_MANIFEST.json';
const frozenBytes=fs.readFileSync(path.join(own,frozen));if(!frozenBytes.equals(git(['show','c57746f82558dd698d13fe5b2fc43dd18d72af6c:'+frozen])))throw Error('FROZEN_REPORT_CHANGED');
for(const artifact of JSON.parse(frozenBytes).artifacts){const file=path.join(path.dirname(frozen),artifact.path),saved=fs.readFileSync(path.join(own,file));
 if(sha(saved)!==artifact.sha256||!saved.equals(git(['show','c57746f82558dd698d13fe5b2fc43dd18d72af6c:'+file.replaceAll('\\','/')])))throw Error('FROZEN_REPORT_ARTIFACT_CHANGED '+file);}
fs.mkdirSync(path.join(out,'.merge-inputs'),{recursive:true});fs.writeFileSync(path.join(out,'.gitignore'),'.merge-inputs/\n');
const receipts=[],resolutions=[];
const moneyProofPath=path.resolve(own,'../healthroster-v2-money-build-13102026/docs/healthroster-build/proposals/strict-qualification-install1-forward/BROKER_INSTALL1_ENTIRE_SOURCE_REVERSE.json');
const moneyProofBytes=fs.readFileSync(moneyProofPath),money=JSON.parse(moneyProofBytes);
if(money.input_commit!==current||money.input_file!=='broker/src/index.js'||money.hunks.length!==2||money.direct_rpc_callers!==2||money.native_recording_rpc_callers!==5)throw Error('MONEY_ADAPTER_SOURCE_CHANGED');
for(const h of money.hunks)if(sha(h.before)!==h.before_sha256||sha(h.after)!==h.after_sha256)throw Error('MONEY_ADAPTER_HUNK_CHANGED');
for(const file of files){const inputs={common:git(['show',common+':'+file]),current:git(['show',current+':'+file]),broad:git(['show',broad+':'+file])},i2Bytes=git(['show',i2+':'+file]);
 if(!i2Bytes.equals(inputs.current))throw Error('I2_BASELINE_NOT_CURRENT '+file);
 const baseName=file.replaceAll('/','__');for(const [kind,body]of Object.entries(inputs))fs.writeFileSync(path.join(out,'.merge-inputs',kind+'__'+baseName),body);
 const merge=spawnSync('git',['merge-file','--diff3','--marker-size=20','-p',path.join(out,'.merge-inputs','current__'+baseName),
  path.join(out,'.merge-inputs','common__'+baseName),path.join(out,'.merge-inputs','broad__'+baseName)],{cwd:own,encoding:null,maxBuffer:64*1024*1024});
 if(merge.status===null||merge.status>127||merge.status<0)throw Error('THREE_WAY_MERGE_FAILED '+file);
 let joined=merge.stdout.toString('utf8');
 for(const match of [...joined.matchAll(/^<{20} [^\r\n]*\r?\n([\s\S]*?)^\|{20} [^\r\n]*\r?\n([\s\S]*?)^={20}\r?\n([\s\S]*?)^>{20} [^\r\n]*(?:\r?\n|$)/gm)]){
  const [block,currentPart,commonPart,broadPart]=match;let selected,reason;
  if(file==='broker/src/index.js'&&currentPart.trim()==="import { handleBpayNextPayMethodChange, createBpayNextPayMethodChangeRpc } from './bpay-next-pay-method-change.js';"
    &&broadPart.trim()==="import { validateLegacyInvoiceManifestSuppressions } from './weekly-source/invoice-render-qualification.js';"&&!commonPart.trim()){
   selected=currentPart+broadPart;reason='Retain current native pay-method owner import and add reviewed broad native renderer qualification import.';
  }else if(file==='broker/src/weekly-source/routes.js'&&currentPart.includes("import { WeeklyProtectedActionError } from './protected-action-orchestrator.mjs';")
    &&currentPart.includes("import { normalizeBpayNextOwnerStatus } from '../bpay-next-legacy-fence.js';")&&!commonPart.trim()
    &&broadPart.trim()==="import { validateRuleSetValues } from './rule-set-validation.js';\nimport { isActivePaymentActor } from '../payment-actor-permission.js';"){
   selected=currentPart+"import { validateRuleSetValues } from './rule-set-validation.js';\n";reason='Retain current native action/fence/payment imports, deduplicate the identical payment import, add reviewed broad rule validator.';
  }else if(file==='broker/src/weekly-source/routes.js'&&!commonPart.trim()&&currentPart.startsWith(broadPart.trimEnd())
    &&currentPart.includes("if (action === 'LATER_CHANGE_DECISION')")){
   selected=currentPart;reason='The broad pay-view gate is already present byte-exact at the start of the current full command arm; preserve the entire current native Later Change Decision body.';
  }else throw Error('UNREVIEWED_OVERLAP '+file);
  if(joined.split(block).length!==2)throw Error('CONFLICT_NOT_EXACT');joined=joined.replace(block,()=>selected);
  resolutions.push({file,conflict_block:block,current:currentPart,common:commonPart,broad:broadPart,selected,reason,
   current_sha256:sha(currentPart),broad_sha256:sha(broadPart),selected_sha256:sha(selected)});
 }
 if(/^[<|=>]{20}(?: |\r?$)/m.test(joined))throw Error('UNRESOLVED_CONFLICT '+file);
 const joinedBeforeMoney=joined;
 if(file==='broker/src/index.js')for(const h of money.hunks){if(joined.split(h.before).length!==2)throw Error('MONEY_ADAPTER_NOT_UNIQUE');joined=joined.replace(h.before,()=>h.after);}
 const target=path.join(out,'forward',file);fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,joined);
 receipts.push({file,current_commit:current,current_sha256:sha(inputs.current),common_commit:common,common_sha256:sha(inputs.common),
  broad_commit:broad,broad_sha256:sha(inputs.broad),i2_commit:i2,i2_git_equals_current:true,three_way_exit:merge.status,
  conflicts:(merge.stdout.toString().match(/^<{20} /gm)??[]).length,three_way_sha256:sha(merge.stdout),joined_before_money_sha256:sha(joinedBeforeMoney),joined_sha256:sha(joined),output:'forward/'+file});
}
fs.writeFileSync(path.join(out,'THREE_WAY_INPUTS.json'),JSON.stringify({schema_version:'INSTALL2_SHARED_RUNTIME_THREE_WAY_SOURCE_V1',status:'SOURCE_THREE_WAY_OVERLAPS_RESOLVED_FULL_REVERSE_PENDING',
 current_commit:current,common_commit:common,broad_commit:broad,i2_commit:i2,frozen_report_commit:'c57746f82558dd698d13fe5b2fc43dd18d72af6c',
 frozen_report_manifest_sha256:sha(frozenBytes),files:receipts,resolutions,money_adapter:{source_artifact:'strict-qualification-install1-forward/BROKER_INSTALL1_ENTIRE_SOURCE_REVERSE.json',
  artifact_sha256:sha(moneyProofBytes),hunks:money.hunks,direct_rpc_callers:2,native_recording_rpc_callers:5,actor_callback_count:1},
 no_saved_i2_or_current_worktree_read_or_mutation:true,no_native_execution:true},null,2)+'\n');
console.log(JSON.stringify(receipts.map(row=>({file:row.file,conflicts:row.conflicts,current_sha256:row.current_sha256,three_way_sha256:row.three_way_sha256}))));
